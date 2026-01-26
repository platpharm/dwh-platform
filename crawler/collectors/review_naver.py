"""
네이버 검색 API 기반 약국 리뷰 수집기

중요: 네이버 Maps/Places API는 리뷰 데이터를 공식 제공하지 않습니다.
대안으로 네이버 검색 API를 활용하여 블로그/카페에서 약국 관련 리뷰를 간접 검색합니다.

API 문서: https://developers.naver.com/docs/serviceapi/search/blog/blog.md
주의: 크롤링은 법적 문제가 있으므로 공식 API만 사용합니다.
"""

import re
import html
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional

from crawler.base_collector import BaseCollector
from crawler.config import CONFIG


@dataclass
class SearchQuery:
    """검색 쿼리 정보"""
    pharmacy_name: str
    address: Optional[str] = None
    keyword_type: str = "후기"  # "후기" or "리뷰"

    def to_query_string(self) -> str:
        """검색 쿼리 문자열 생성"""
        query = f"{self.pharmacy_name} {self.keyword_type}"
        if self.address:
            # 주소에서 시/군/구 추출하여 검색 정확도 향상
            address_parts = self.address.split()
            if len(address_parts) >= 2:
                query = f"{address_parts[0]} {address_parts[1]} {self.pharmacy_name} {self.keyword_type}"
        return query


class ReviewNaverCollector(BaseCollector):
    """
    네이버 검색 API를 활용한 약국 리뷰 수집기

    네이버 Maps/Places API가 리뷰를 제공하지 않으므로,
    블로그/카페 검색 API로 약국 관련 리뷰를 간접 수집합니다.

    Attributes:
        ES_INDEX (str): Elasticsearch 인덱스 이름
        SEARCH_TYPES (list): 검색 API 유형 (blog, cafearticle)
        KEYWORDS (list): 검색에 사용할 키워드
    """

    # Elasticsearch 인덱스
    ES_INDEX = "review_naver"

    # 검색 API 유형
    SEARCH_TYPES = ["blog", "cafearticle"]

    # 검색 키워드
    KEYWORDS = ["후기", "리뷰"]

    # API 설정
    NAVER_SEARCH_API_BASE = "https://openapi.naver.com/v1/search"
    DEFAULT_DISPLAY = 100  # 한 번에 가져올 결과 수 (최대 100)
    MAX_START = 1000  # 네이버 API 최대 시작 위치 제한

    def __init__(self, name: str = "ReviewNaverCollector"):
        """
        ReviewNaverCollector 초기화

        Args:
            name: 수집기 이름 (로깅용)
        """
        super().__init__(name=name)
        self.client_id = CONFIG["naver"]["client_id"]
        self.client_secret = CONFIG["naver"]["client_secret"]

        # API 키 검증
        if not self.client_id or not self.client_secret:
            self.logger.warning(
                "네이버 API 키가 설정되지 않았습니다. "
                "NAVER_CLIENT_ID, NAVER_CLIENT_SECRET 환경변수를 확인하세요."
            )

    def _get_naver_headers(self) -> Dict[str, str]:
        """
        네이버 API 인증 헤더 반환

        Returns:
            인증 헤더 딕셔너리
        """
        return {
            "X-Naver-Client-Id": self.client_id,
            "X-Naver-Client-Secret": self.client_secret,
        }

    def _clean_html_text(self, text: str) -> str:
        """
        HTML 태그 및 특수문자 제거

        Args:
            text: 원본 텍스트

        Returns:
            정제된 텍스트
        """
        if not text:
            return ""

        # HTML 엔티티 디코딩
        text = html.unescape(text)

        # HTML 태그 제거
        text = re.sub(r"<[^>]+>", "", text)

        # 연속 공백 정리
        text = re.sub(r"\s+", " ", text)

        return text.strip()

    def _parse_date(self, date_str: str) -> Optional[str]:
        """
        날짜 문자열을 ISO 형식으로 변환

        Args:
            date_str: 원본 날짜 문자열 (예: "20240115", "2024-01-15")

        Returns:
            ISO 형식 날짜 문자열 또는 None
        """
        if not date_str:
            return None

        try:
            # 네이버 API 블로그 날짜 형식: "20240115"
            if len(date_str) == 8 and date_str.isdigit():
                parsed = datetime.strptime(date_str, "%Y%m%d")
                return parsed.strftime("%Y-%m-%d")

            # 이미 ISO 형식인 경우
            if "-" in date_str:
                return date_str

            return None
        except ValueError:
            return None

    def _search_naver(
        self,
        search_type: str,
        query: str,
        display: int = 100,
        start: int = 1,
        sort: str = "sim"
    ) -> Optional[Dict[str, Any]]:
        """
        네이버 검색 API 호출

        Args:
            search_type: 검색 유형 (blog, cafearticle)
            query: 검색어
            display: 결과 개수 (1-100)
            start: 시작 위치 (1-1000)
            sort: 정렬 방식 (sim: 유사도, date: 날짜순)

        Returns:
            API 응답 딕셔너리 또는 None
        """
        url = f"{self.NAVER_SEARCH_API_BASE}/{search_type}"

        params = {
            "query": query,
            "display": min(display, 100),
            "start": min(start, self.MAX_START),
            "sort": sort,
        }

        response = self._make_request(
            url=url,
            params=params,
            headers=self._get_naver_headers(),
            max_retries=2
        )

        if response and response.status_code == 200:
            return self._parse_json_response(response)

        if response:
            self.logger.error(
                f"네이버 검색 API 오류: status={response.status_code}, "
                f"response={response.text[:200]}"
            )

        return None

    def _search_blog(
        self,
        query: str,
        display: int = 100,
        start: int = 1,
        sort: str = "sim"
    ) -> List[Dict[str, Any]]:
        """
        네이버 블로그 검색

        Args:
            query: 검색어
            display: 결과 개수
            start: 시작 위치
            sort: 정렬 방식

        Returns:
            블로그 검색 결과 리스트
        """
        result = self._search_naver("blog", query, display, start, sort)

        if not result or "items" not in result:
            return []

        items = []
        for item in result.get("items", []):
            items.append({
                "source_type": "blog",
                "title": self._clean_html_text(item.get("title", "")),
                "description": self._clean_html_text(item.get("description", "")),
                "link": item.get("link", ""),
                "blogger_name": item.get("bloggername", ""),
                "blogger_link": item.get("bloggerlink", ""),
                "post_date": self._parse_date(item.get("postdate", "")),
            })

        return items

    def _search_cafe(
        self,
        query: str,
        display: int = 100,
        start: int = 1,
        sort: str = "sim"
    ) -> List[Dict[str, Any]]:
        """
        네이버 카페 검색

        Args:
            query: 검색어
            display: 결과 개수
            start: 시작 위치
            sort: 정렬 방식

        Returns:
            카페 검색 결과 리스트
        """
        result = self._search_naver("cafearticle", query, display, start, sort)

        if not result or "items" not in result:
            return []

        items = []
        for item in result.get("items", []):
            items.append({
                "source_type": "cafe",
                "title": self._clean_html_text(item.get("title", "")),
                "description": self._clean_html_text(item.get("description", "")),
                "link": item.get("link", ""),
                "cafe_name": item.get("cafename", ""),
                "cafe_url": item.get("cafeurl", ""),
            })

        return items

    def search_pharmacy_reviews(
        self,
        pharmacy_name: str,
        address: Optional[str] = None,
        max_results: int = 100
    ) -> List[Dict[str, Any]]:
        """
        특정 약국에 대한 리뷰 검색

        Args:
            pharmacy_name: 약국명
            address: 약국 주소 (선택, 검색 정확도 향상용)
            max_results: 최대 결과 수

        Returns:
            검색된 리뷰 리스트
        """
        all_results = []

        for keyword in self.KEYWORDS:
            search_query = SearchQuery(
                pharmacy_name=pharmacy_name,
                address=address,
                keyword_type=keyword
            )
            query_string = search_query.to_query_string()

            self.logger.debug(f"검색 쿼리: {query_string}")

            # 블로그 검색
            blog_results = self._search_blog(
                query=query_string,
                display=min(max_results, self.DEFAULT_DISPLAY),
                sort="date"  # 최신순
            )

            for item in blog_results:
                item["query"] = query_string
                item["pharmacy_name"] = pharmacy_name
                item["pharmacy_address"] = address
                item["search_keyword"] = keyword
                item["collected_at"] = datetime.utcnow().isoformat()

            all_results.extend(blog_results)

            # 카페 검색
            cafe_results = self._search_cafe(
                query=query_string,
                display=min(max_results, self.DEFAULT_DISPLAY),
                sort="date"
            )

            for item in cafe_results:
                item["query"] = query_string
                item["pharmacy_name"] = pharmacy_name
                item["pharmacy_address"] = address
                item["search_keyword"] = keyword
                item["collected_at"] = datetime.utcnow().isoformat()

            all_results.extend(cafe_results)

        # 중복 제거 (링크 기준)
        seen_links = set()
        unique_results = []
        for item in all_results:
            link = item.get("link", "")
            if link and link not in seen_links:
                seen_links.add(link)
                unique_results.append(item)

        self.logger.info(
            f"[{pharmacy_name}] 검색 완료: {len(unique_results)}건 "
            f"(블로그+카페, 중복 제거)"
        )

        return unique_results

    def collect(
        self,
        pharmacies: Optional[List[Dict[str, Any]]] = None,
        max_results_per_pharmacy: int = 50
    ) -> List[Dict[str, Any]]:
        """
        약국 리뷰 데이터 수집

        Args:
            pharmacies: 약국 정보 리스트 (name, address 포함)
                        None인 경우 샘플 데이터로 테스트
            max_results_per_pharmacy: 약국당 최대 검색 결과 수

        Returns:
            수집된 리뷰 데이터 리스트
        """
        # API 키 확인
        if not self.client_id or not self.client_secret:
            self.logger.error(
                "네이버 API 키가 설정되지 않아 수집을 중단합니다. "
                "환경변수 NAVER_CLIENT_ID, NAVER_CLIENT_SECRET를 설정하세요."
            )
            return []

        # 약국 리스트가 없으면 샘플 데이터로 테스트
        if pharmacies is None:
            self.logger.info("약국 리스트가 제공되지 않아 샘플 데이터로 테스트합니다.")
            pharmacies = [
                {"name": "온누리약국", "address": "서울특별시 강남구"},
                {"name": "참조은약국", "address": "서울특별시 서초구"},
            ]

        all_reviews = []

        for idx, pharmacy in enumerate(pharmacies, 1):
            pharmacy_name = pharmacy.get("name", "")
            pharmacy_address = pharmacy.get("address", "")

            if not pharmacy_name:
                self.logger.warning(f"약국명이 없어 건너뜁니다: {pharmacy}")
                continue

            self.logger.info(
                f"[{idx}/{len(pharmacies)}] {pharmacy_name} 리뷰 검색 중..."
            )

            try:
                reviews = self.search_pharmacy_reviews(
                    pharmacy_name=pharmacy_name,
                    address=pharmacy_address,
                    max_results=max_results_per_pharmacy
                )
                all_reviews.extend(reviews)

            except Exception as e:
                self.logger.error(f"[{pharmacy_name}] 검색 중 오류 발생: {e}")
                continue

        self.logger.info(f"총 {len(all_reviews)}건의 리뷰 수집 완료")
        return all_reviews

    def collect_and_save(
        self,
        pharmacies: Optional[List[Dict[str, Any]]] = None,
        max_results_per_pharmacy: int = 50
    ) -> Dict[str, int]:
        """
        리뷰 수집 후 Elasticsearch에 저장

        Args:
            pharmacies: 약국 정보 리스트
            max_results_per_pharmacy: 약국당 최대 검색 결과 수

        Returns:
            저장 결과 통계
        """
        reviews = self.collect(
            pharmacies=pharmacies,
            max_results_per_pharmacy=max_results_per_pharmacy
        )

        if not reviews:
            return {"success": 0, "failed": 0}

        return self.save_to_es(
            data=reviews,
            index_name=self.ES_INDEX,
            id_field=None  # 자동 생성 ID 사용
        )


# 모듈 직접 실행 시 테스트
if __name__ == "__main__":
    import logging

    # 로깅 레벨 설정
    logging.basicConfig(level=logging.INFO)

    # 수집기 인스턴스 생성
    collector = ReviewNaverCollector()

    # 테스트 실행 (API 키가 설정된 경우에만 동작)
    results = collector.collect()

    print(f"\n=== 수집 결과 ===")
    print(f"총 {len(results)}건 수집")

    if results:
        print(f"\n샘플 데이터 (첫 번째 항목):")
        for key, value in results[0].items():
            print(f"  {key}: {value}")
