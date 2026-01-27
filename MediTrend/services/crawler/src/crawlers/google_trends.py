"""구글 트렌드 크롤러 (pytrends 사용)"""

import logging
import re
import time
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Tuple

from pytrends.request import TrendReq
from tenacity import retry, stop_after_attempt, wait_exponential

from shared.clients.es_client import es_client
from shared.models.schemas import TrendData
from shared.config import ESIndex

logger = logging.getLogger(__name__)

# 카테고리2 코드 매핑 (상품명 검색량 부족 시 대체 검색용)
CATEGORY2_MAPPING = {
    "01": "해열진통소염제",
    "02": "항히스타민제",
    "03": "소화기관용제",
    "04": "비타민제",
    "05": "호르몬제",
    "06": "외용제",
    "07": "안과용제",
    "08": "이비과용제",
    "09": "항생물질제제",
    "10": "항바이러스제",
    "11": "순환계용제",
    "12": "호흡기관용제",
    "13": "비뇨생식기관용제",
    "14": "중추신경용제",
    "15": "말초신경용제",
    "16": "조직세포의기능용제",
    "17": "대사성의약품",
    "18": "자양강장변질제",
    "19": "진단용약",
    "20": "기타의약품",
}


class GoogleTrendsCrawler:
    """구글 트렌드 크롤러 (pytrends 라이브러리 사용)"""

    def __init__(self, hl: str = "ko", tz: int = 540, geo: str = "KR"):
        """
        Args:
            hl: 언어 설정 (default: 한국어)
            tz: 타임존 오프셋 (default: 540 = UTC+9, 한국)
            geo: 지역 설정 (default: 한국)
        """
        self.hl = hl
        self.tz = tz
        self.geo = geo
        self.pytrends = TrendReq(hl=hl, tz=tz)

    def _get_timeframe(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> str:
        """timeframe 문자열 생성"""
        if not start_date:
            start_date = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
        if not end_date:
            end_date = datetime.now().strftime("%Y-%m-%d")

        return f"{start_date} {end_date}"

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
    )
    def fetch_interest_over_time(
        self,
        keywords: List[str],
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> Dict[str, Any]:
        """시간에 따른 관심도 데이터 가져오기"""
        timeframe = self._get_timeframe(start_date, end_date)
        results = {}

        # pytrends는 한 번에 최대 5개 키워드만 처리 가능
        for i in range(0, len(keywords), 5):
            batch_keywords = keywords[i : i + 5]

            self.pytrends.build_payload(
                kw_list=batch_keywords,
                cat=0,  # 모든 카테고리
                timeframe=timeframe,
                geo=self.geo,
            )

            interest_df = self.pytrends.interest_over_time()

            if not interest_df.empty:
                if "isPartial" in interest_df.columns:
                    interest_df = interest_df.drop(columns=["isPartial"])

                for keyword in batch_keywords:
                    if keyword in interest_df.columns:
                        results[keyword] = interest_df[keyword].to_dict()

            if i + 5 < len(keywords):
                time.sleep(1)

        return results

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
    )
    def fetch_related_queries(
        self,
        keywords: List[str],
    ) -> Dict[str, Dict[str, Any]]:
        """연관 검색어 가져오기"""
        results = {}

        for i in range(0, len(keywords), 5):
            batch_keywords = keywords[i : i + 5]

            self.pytrends.build_payload(
                kw_list=batch_keywords,
                cat=0,
                timeframe="today 3-m",
                geo=self.geo,
            )

            related = self.pytrends.related_queries()

            for keyword in batch_keywords:
                if keyword in related:
                    keyword_data = related[keyword]
                    results[keyword] = {
                        "top": (
                            keyword_data["top"].to_dict("records")
                            if keyword_data["top"] is not None
                            else []
                        ),
                        "rising": (
                            keyword_data["rising"].to_dict("records")
                            if keyword_data["rising"] is not None
                            else []
                        ),
                    }

            if i + 5 < len(keywords):
                time.sleep(1)

        return results

    def _parse_trend_data(
        self,
        interest_data: Dict[str, Dict],
        related_data: Optional[Dict[str, Dict]] = None,
    ) -> List[TrendData]:
        """API 응답을 TrendData 스키마로 변환"""
        trend_data_list = []

        for keyword, time_series in interest_data.items():
            for timestamp, score in time_series.items():
                if hasattr(timestamp, "to_pydatetime"):
                    dt = timestamp.to_pydatetime()
                elif isinstance(timestamp, str):
                    dt = datetime.fromisoformat(timestamp)
                else:
                    dt = timestamp

                metadata = {"score": score}

                related_keywords = None
                if related_data and keyword in related_data:
                    metadata["related_queries"] = related_data[keyword]
                    # Extract related keywords from top queries
                    top_queries = related_data[keyword].get("top", [])
                    if top_queries:
                        related_keywords = [q.get("query", "") for q in top_queries[:10] if q.get("query")]

                trend_data = TrendData(
                    keyword=keyword,
                    source="google",
                    value=float(score),
                    date=dt,
                    related_keywords=related_keywords,
                    metadata=metadata,
                )
                trend_data_list.append(trend_data)

        return trend_data_list

    def crawl_and_save(
        self,
        keywords: List[str],
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        include_related: bool = True,
    ) -> int:
        """크롤링 후 ES에 저장"""
        interest_data = self.fetch_interest_over_time(keywords, start_date, end_date)

        related_data = None
        if include_related:
            related_data = self.fetch_related_queries(keywords)

        trend_data_list = self._parse_trend_data(interest_data, related_data)

        if not trend_data_list:
            return 0

        documents = [
            {
                **data.model_dump(),
                "date": data.date.isoformat(),
            }
            for data in trend_data_list
        ]

        success, _ = es_client.bulk_index(
            index=ESIndex.TREND_DATA,
            documents=documents,
        )

        return success

    def get_top_products_from_es(
        self,
        top_n: int = 100,
    ) -> List[Dict[str, Any]]:
        """
        CDC ES에서 인기 상품 Top N개 조회

        Args:
            top_n: 가져올 상품 수 (기본값: 100)

        Returns:
            상품 정보 리스트 [{product_id, product_name, category2, ...}, ...]
        """
        logger.info(f"Fetching top {top_n} products from CDC ES")

        try:
            query = {"match_all": {}}

            # ranking_result 인덱스에서 인기 상품 조회, 없으면 CDC product에서 전체 조회
            ranking_products = []
            try:
                ranking_query = {"match_all": {}}
                ranking_results = es_client.search(
                    index=ESIndex.RANKING_RESULT,
                    query=ranking_query,
                    size=top_n,
                    source=["product_id", "product_name", "category2", "popularity_score"]
                )
                if ranking_results:
                    ranking_products = ranking_results
                    logger.info(f"Found {len(ranking_products)} products from ranking index")
            except Exception as e:
                logger.warning(f"Ranking index not available, falling back to CDC product: {e}")

            if ranking_products:
                return ranking_products

            products = es_client.search(
                index=ESIndex.CDC_PRODUCT,
                query=query,
                size=top_n,
                source=["id", "name", "category2", "efficacy", "ingredient"]
            )

            result = []
            for p in products:
                result.append({
                    "product_id": p.get("id"),
                    "product_name": p.get("name"),
                    "category2": p.get("category2"),
                    "efficacy": p.get("efficacy"),
                    "ingredient": p.get("ingredient"),
                })

            logger.info(f"Fetched {len(result)} products from CDC ES")
            return result

        except Exception as e:
            logger.error(f"Failed to fetch products from ES: {e}")
            return []

    def _normalize_product_name_for_search(self, name: str) -> str:
        """
        상품명을 Google Trends 검색에 적합하게 정규화

        Args:
            name: 원본 상품명

        Returns:
            정규화된 상품명
        """
        if not name:
            return ""

        normalized = re.sub(r"\([^)]*\)", "", name)
        normalized = re.sub(
            r"\d+\.?\d*\s*(mg|ml|g|L|정|캡슐|포|개|mcg|iu|%)",
            "",
            normalized,
            flags=re.IGNORECASE
        )
        normalized = re.sub(
            r"(필름코팅정|정제|캡슐|연질캡슐|시럽|액|주사|연고|크림|겔|패치|필름|과립|산제)",
            "",
            normalized
        )
        normalized = re.sub(r"[^\w\s가-힣]", "", normalized)
        normalized = re.sub(r"\s+", " ", normalized).strip()

        return normalized

    def _get_category_name(self, category2: Optional[str]) -> Optional[str]:
        """카테고리2 코드를 카테고리명으로 변환"""
        if not category2:
            return None
        return CATEGORY2_MAPPING.get(str(category2).zfill(2))

    def fetch_product_trends(
        self,
        products: List[Dict[str, Any]],
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        min_trend_value: float = 5.0,
    ) -> Tuple[Dict[str, Any], Dict[str, Dict]]:
        """
        상품명으로 트렌드 검색, 검색량 부족 시 카테고리명으로 대체

        Args:
            products: 상품 정보 리스트
            start_date: 검색 시작일
            end_date: 검색 종료일
            min_trend_value: 트렌드 값이 이 값 미만이면 카테고리로 대체 검색

        Returns:
            (interest_data, product_keyword_mapping)
            - interest_data: 키워드별 트렌드 데이터
            - product_keyword_mapping: product_id -> {keyword, is_category_fallback}
        """
        logger.info(f"Fetching trends for {len(products)} products")

        product_keywords = {}
        product_category_map = {}

        for p in products:
            product_id = p.get("product_id") or p.get("id")
            product_name = p.get("product_name") or p.get("name")
            category2 = p.get("category2")

            if not product_id or not product_name:
                continue

            normalized = self._normalize_product_name_for_search(product_name)
            if normalized:
                if normalized not in product_keywords:
                    product_keywords[normalized] = []
                product_keywords[normalized].append(product_id)

            category_name = self._get_category_name(category2)
            if category_name:
                product_category_map[product_id] = category_name

        keywords = list(product_keywords.keys())
        logger.info(f"Unique keywords to search: {len(keywords)}")

        interest_data = self.fetch_interest_over_time(
            keywords=keywords,
            start_date=start_date,
            end_date=end_date,
        )

        product_keyword_mapping = {}
        category_fallback_keywords = set()

        for keyword, product_ids in product_keywords.items():
            if keyword in interest_data:
                values = list(interest_data[keyword].values())
                avg_value = sum(values) / len(values) if values else 0

                for pid in product_ids:
                    if avg_value >= min_trend_value:
                        product_keyword_mapping[pid] = {
                            "keyword": keyword,
                            "is_category_fallback": False,
                            "avg_trend_value": avg_value,
                        }
                    else:
                        category_name = product_category_map.get(pid)
                        if category_name:
                            category_fallback_keywords.add(category_name)
                            product_keyword_mapping[pid] = {
                                "keyword": category_name,
                                "is_category_fallback": True,
                                "original_keyword": keyword,
                                "avg_trend_value": avg_value,
                            }
            else:
                for pid in product_ids:
                    category_name = product_category_map.get(pid)
                    if category_name:
                        category_fallback_keywords.add(category_name)
                        product_keyword_mapping[pid] = {
                            "keyword": category_name,
                            "is_category_fallback": True,
                            "original_keyword": keyword,
                            "avg_trend_value": 0,
                        }

        if category_fallback_keywords:
            logger.info(f"Fetching category fallback trends for {len(category_fallback_keywords)} categories")
            category_trends = self.fetch_interest_over_time(
                keywords=list(category_fallback_keywords),
                start_date=start_date,
                end_date=end_date,
            )
            interest_data.update(category_trends)

        return interest_data, product_keyword_mapping

    def crawl_product_trends_and_save(
        self,
        top_n: int = 100,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        include_related: bool = True,
    ) -> Dict[str, Any]:
        """
        상품명 기반 트렌드 크롤링 후 ES에 저장

        Args:
            top_n: 검색할 상위 상품 수
            start_date: 검색 시작일 (YYYY-MM-DD)
            end_date: 검색 종료일 (YYYY-MM-DD)
            include_related: 연관 검색어 포함 여부

        Returns:
            처리 결과 딕셔너리
        """
        logger.info(f"Starting product-based trend crawling for top {top_n} products")

        products = self.get_top_products_from_es(top_n=top_n)
        if not products:
            logger.warning("No products found in ES")
            return {
                "success": False,
                "trend_count": 0,
                "mapping_count": 0,
                "message": "No products found in ES",
            }

        interest_data, product_keyword_mapping = self.fetch_product_trends(
            products=products,
            start_date=start_date,
            end_date=end_date,
        )

        related_data = None
        if include_related and interest_data:
            all_keywords = list(interest_data.keys())
            related_data = self.fetch_related_queries(all_keywords)

        trend_data_list = self._parse_trend_data(interest_data, related_data)
        trend_count = 0
        if trend_data_list:
            documents = [
                {
                    **data.model_dump(),
                    "date": data.date.isoformat(),
                }
                for data in trend_data_list
            ]
            trend_count, _ = es_client.bulk_index(
                index=ESIndex.TREND_DATA,
                documents=documents,
            )

        mapping_documents = []
        for product_id, mapping_info in product_keyword_mapping.items():
            product_info = next(
                (p for p in products if (p.get("product_id") or p.get("id")) == product_id),
                {}
            )
            mapping_documents.append({
                "product_id": product_id,
                "product_name": product_info.get("product_name") or product_info.get("name"),
                "keyword": mapping_info["keyword"],
                "keyword_source": "category" if mapping_info["is_category_fallback"] else "product_name",
                "is_category_fallback": mapping_info["is_category_fallback"],
                "original_keyword": mapping_info.get("original_keyword"),
                "avg_trend_value": mapping_info.get("avg_trend_value", 0),
                "timestamp": datetime.now().isoformat(),
            })

        mapping_count = 0
        if mapping_documents:
            mapping_count, _ = es_client.bulk_index(
                index=ESIndex.TREND_PRODUCT_MAPPING,
                documents=mapping_documents,
                id_field="product_id",
            )

        logger.info(f"Saved {trend_count} trend records and {mapping_count} mappings")

        return {
            "success": True,
            "trend_count": trend_count,
            "mapping_count": mapping_count,
            "products_processed": len(products),
            "unique_keywords": len(interest_data),
            "category_fallbacks": sum(1 for m in product_keyword_mapping.values() if m["is_category_fallback"]),
            "message": f"상품명 기반 트렌드 {trend_count}건, 매핑 {mapping_count}건 저장 완료",
        }


google_crawler = GoogleTrendsCrawler()
