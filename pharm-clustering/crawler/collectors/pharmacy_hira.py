"""
건강보험심사평가원 약국 정보 수집기 (Pharmacy HIRA Collector)

건강보험심사평가원의 약국 기본정보 조회 서비스 API를 사용하여
전국 약국 정보를 수집합니다.

API 정보:
    - Base URL: http://apis.data.go.kr/B551182/pharmacyInfoService
    - Endpoint: /getParmacyBasisList
    - 인증: data.go.kr API Key (serviceKey)

ES 인덱스: pharmacy_hira
"""

import sys
import time
from typing import Any, Dict, List, Optional
from xml.etree import ElementTree

# 패키지 경로 임포트
from crawler.base_collector import BaseCollector
from crawler.config import CONFIG


class PharmacyHIRACollector(BaseCollector):
    """
    건강보험심사평가원 약국 정보 수집기

    전국 시도별로 순회하며 모든 약국 정보를 수집합니다.

    Attributes:
        BASE_URL (str): API Base URL
        ENDPOINT (str): API Endpoint
        ES_INDEX (str): Elasticsearch 인덱스명
        PAGE_SIZE (int): 한 페이지당 조회 건수
    """

    BASE_URL = "http://apis.data.go.kr/B551182/pharmacyInfoService"
    ENDPOINT = "/getParmacyBasisList"
    ES_INDEX = "pharmacy_hira"
    PAGE_SIZE = 100

    # 시도 코드 (건강보험심사평가원 기준)
    # sidoCd 파라미터에 사용
    SIDO_CODES = {
        "110000": "서울",
        "210000": "부산",
        "220000": "인천",
        "230000": "대구",
        "240000": "광주",
        "250000": "대전",
        "260000": "울산",
        "310000": "경기",
        "320000": "강원",
        "330000": "충북",
        "340000": "충남",
        "350000": "전북",
        "360000": "전남",
        "370000": "경북",
        "380000": "경남",
        "390000": "제주",
        "410000": "세종",
    }

    def __init__(self):
        """PharmacyHIRACollector 초기화"""
        super().__init__(name="PharmacyHIRACollector")
        self.service_key = CONFIG["data_go_kr"]["service_key"]

        if not self.service_key:
            self.logger.warning(
                "DATA_GO_KR_API_KEY가 설정되지 않았습니다. "
                ".env 파일을 확인하세요."
            )

    def collect(self) -> List[Dict[str, Any]]:
        """
        전국 약국 정보 수집

        전국 시도별로 순회하며 모든 약국 정보를 수집합니다.
        페이지네이션을 통해 시도별 전체 데이터를 수집합니다.

        Returns:
            수집된 약국 정보 리스트
        """
        all_pharmacies = []

        self.logger.info("건강보험심사평가원 약국 정보 수집 시작")

        for sido_cd, sido_nm in self.SIDO_CODES.items():
            self.logger.info(f"[{sido_nm}] 약국 수집 시작 (sidoCd={sido_cd})")

            pharmacies = self._collect_by_sido(sido_cd, sido_nm)
            all_pharmacies.extend(pharmacies)

            self.logger.info(f"[{sido_nm}] 약국 수집 완료: {len(pharmacies)}건")

            # API 호출 부하 방지를 위한 딜레이
            time.sleep(0.1)

        self.logger.info(f"전체 약국 수집 완료: 총 {len(all_pharmacies)}건")

        return all_pharmacies

    def _collect_by_sido(
        self,
        sido_cd: str,
        sido_nm: str
    ) -> List[Dict[str, Any]]:
        """
        특정 시도의 모든 약국 정보 수집

        Args:
            sido_cd: 시도 코드
            sido_nm: 시도 이름 (로깅용)

        Returns:
            해당 시도의 약국 정보 리스트
        """
        pharmacies = []
        page_no = 1
        total_count = None

        while True:
            # API 호출
            result = self._request_pharmacy_list(sido_cd, page_no)

            if result is None:
                self.logger.error(
                    f"[{sido_nm}] 페이지 {page_no} 조회 실패, 다음 시도로 이동"
                )
                break

            # 전체 건수 확인 (첫 페이지에서만)
            if total_count is None:
                total_count = result.get("total_count", 0)
                self.logger.info(f"[{sido_nm}] 전체 약국 수: {total_count}건")

            # 데이터 추출
            items = result.get("items", [])
            if not items:
                break

            pharmacies.extend(items)

            # 다음 페이지 확인
            if len(pharmacies) >= total_count:
                break

            page_no += 1

            # API 호출 부하 방지를 위한 딜레이
            time.sleep(0.05)

        return pharmacies

    def _request_pharmacy_list(
        self,
        sido_cd: str,
        page_no: int
    ) -> Optional[Dict[str, Any]]:
        """
        약국 목록 API 호출

        Args:
            sido_cd: 시도 코드
            page_no: 페이지 번호

        Returns:
            API 응답 데이터 (items, total_count) 또는 None
        """
        url = f"{self.BASE_URL}{self.ENDPOINT}"

        params = {
            "serviceKey": self.service_key,
            "pageNo": page_no,
            "numOfRows": self.PAGE_SIZE,
            "sidoCd": sido_cd,
        }

        response = self._make_request(url, params=params, max_retries=3)

        if response is None:
            return None

        # XML 응답 파싱
        return self._parse_api_response(response.text)

    def _parse_api_response(
        self,
        xml_text: str
    ) -> Optional[Dict[str, Any]]:
        """
        API XML 응답 파싱

        Args:
            xml_text: XML 응답 텍스트

        Returns:
            파싱된 데이터 (items, total_count) 또는 None
        """
        try:
            root = ElementTree.fromstring(xml_text)

            # 에러 체크
            result_code = root.findtext(".//resultCode")
            if result_code != "00":
                result_msg = root.findtext(".//resultMsg")
                self.logger.error(f"API 에러: {result_code} - {result_msg}")
                return None

            # 전체 건수
            total_count = int(root.findtext(".//totalCount") or 0)

            # 아이템 파싱
            items = []
            for item in root.findall(".//item"):
                pharmacy = self._parse_item(item)
                if pharmacy:
                    items.append(pharmacy)

            return {
                "items": items,
                "total_count": total_count,
            }

        except ElementTree.ParseError as e:
            self.logger.error(f"XML 파싱 오류: {e}")
            return None

    def _parse_item(self, item: ElementTree.Element) -> Optional[Dict[str, Any]]:
        """
        개별 약국 정보 파싱

        Args:
            item: XML item 엘리먼트

        Returns:
            파싱된 약국 정보 딕셔너리
        """
        try:
            pharmacy = {
                # 기본 정보
                "ykiho": item.findtext("ykiho"),  # 암호화된 요양기호 (PK)
                "yadmNm": item.findtext("yadmNm"),  # 약국명
                "clCd": item.findtext("clCd"),  # 종별코드
                "clCdNm": item.findtext("clCdNm"),  # 종별코드명
                # 위치 정보
                "sidoCd": item.findtext("sidoCd"),  # 시도코드
                "sidoCdNm": item.findtext("sidoCdNm"),  # 시도명
                "sgguCd": item.findtext("sgguCd"),  # 시군구코드
                "sgguCdNm": item.findtext("sgguCdNm"),  # 시군구명
                "emdongNm": item.findtext("emdongNm"),  # 읍면동명
                "addr": item.findtext("addr"),  # 주소
                "postNo": item.findtext("postNo"),  # 우편번호
                # 연락처
                "telno": item.findtext("telno"),  # 전화번호
                # 좌표 (WGS84)
                "XPos": self._parse_float(item.findtext("XPos")),  # 경도
                "YPos": self._parse_float(item.findtext("YPos")),  # 위도
                # 기타 정보
                "estbDd": item.findtext("estbDd"),  # 개설일자
                # 운영시간 (요일별)
                "dutyTime1s": item.findtext("dutyTime1s"),  # 월요일 시작
                "dutyTime1c": item.findtext("dutyTime1c"),  # 월요일 종료
                "dutyTime2s": item.findtext("dutyTime2s"),  # 화요일 시작
                "dutyTime2c": item.findtext("dutyTime2c"),  # 화요일 종료
                "dutyTime3s": item.findtext("dutyTime3s"),  # 수요일 시작
                "dutyTime3c": item.findtext("dutyTime3c"),  # 수요일 종료
                "dutyTime4s": item.findtext("dutyTime4s"),  # 목요일 시작
                "dutyTime4c": item.findtext("dutyTime4c"),  # 목요일 종료
                "dutyTime5s": item.findtext("dutyTime5s"),  # 금요일 시작
                "dutyTime5c": item.findtext("dutyTime5c"),  # 금요일 종료
                "dutyTime6s": item.findtext("dutyTime6s"),  # 토요일 시작
                "dutyTime6c": item.findtext("dutyTime6c"),  # 토요일 종료
                "dutyTime7s": item.findtext("dutyTime7s"),  # 일요일 시작
                "dutyTime7c": item.findtext("dutyTime7c"),  # 일요일 종료
                "dutyTime8s": item.findtext("dutyTime8s"),  # 공휴일 시작
                "dutyTime8c": item.findtext("dutyTime8c"),  # 공휴일 종료
            }

            # None 값 필터링 (선택적)
            pharmacy = {k: v for k, v in pharmacy.items() if v is not None}

            return pharmacy

        except Exception as e:
            self.logger.error(f"약국 정보 파싱 오류: {e}")
            return None

    def _parse_float(self, value: Optional[str]) -> Optional[float]:
        """
        문자열을 float로 변환

        Args:
            value: 변환할 문자열

        Returns:
            변환된 float 또는 None
        """
        if value is None or value.strip() == "":
            return None
        try:
            return float(value)
        except ValueError:
            return None

    def run(self) -> Dict[str, int]:
        """
        수집 및 Elasticsearch 저장 실행

        Returns:
            저장 결과 통계 (success, failed 카운트)
        """
        self.logger.info("PharmacyHIRACollector 실행 시작")

        # 데이터 수집
        pharmacies = self.collect()

        if not pharmacies:
            self.logger.warning("수집된 약국 정보가 없습니다.")
            return {"success": 0, "failed": 0}

        # Elasticsearch 저장
        result = self.save_to_es(
            data=pharmacies,
            index_name=self.ES_INDEX,
            id_field="ykiho"  # 암호화된 요양기호를 문서 ID로 사용
        )

        self.logger.info(
            f"PharmacyHIRACollector 실행 완료: "
            f"성공={result['success']}, 실패={result['failed']}"
        )

        return result


# CLI 실행 지원
if __name__ == "__main__":
    collector = PharmacyHIRACollector()
    collector.run()
