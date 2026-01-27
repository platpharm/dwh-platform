"""네이버 데이터랩 검색어 트렌드 크롤러"""

import logging
import os
import re
import time
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Tuple

import requests
from tenacity import retry, stop_after_attempt, wait_exponential

from shared.clients.es_client import es_client
from shared.models.schemas import TrendData
from shared.config import ESIndex

logger = logging.getLogger(__name__)

NCP_DATALAB_URL = "https://naveropenapi.apigw.ntruss.com/datalab/v1/search"
BATCH_SLEEP_SECONDS = 1

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


class NaverTrendsCrawler:
    """네이버 데이터랩 검색어 트렌드 크롤러 (NCP API Gateway)"""

    def __init__(self):
        self.client_id = os.getenv("NAVER_CLIENT_ID", "")
        self.client_secret = os.getenv("NAVER_CLIENT_SECRET", "")

    def _get_headers(self) -> Dict[str, str]:
        return {
            "X-NCP-APIGW-API-KEY-ID": self.client_id,
            "X-NCP-APIGW-API-KEY": self.client_secret,
            "Content-Type": "application/json",
        }

    def _get_date_range(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> Tuple[str, str]:
        if not end_date:
            end_date = datetime.now().strftime("%Y-%m-%d")
        if not start_date:
            start_date = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
        return start_date, end_date

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
    )
    def _request_datalab(self, keyword_groups: List[Dict[str, Any]], start_date: str, end_date: str) -> Dict[str, Any]:
        body = {
            "startDate": start_date,
            "endDate": end_date,
            "timeUnit": "date",
            "keywordGroups": keyword_groups,
        }

        response = requests.post(
            NCP_DATALAB_URL,
            headers=self._get_headers(),
            json=body,
            timeout=30,
        )
        response.raise_for_status()
        return response.json()

    def fetch_interest_over_time(
        self,
        keywords: List[str],
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> Dict[str, Dict[str, float]]:
        """키워드별 시간에 따른 검색 트렌드 데이터 가져오기"""
        start_date, end_date = self._get_date_range(start_date, end_date)
        results: Dict[str, Dict[str, float]] = {}

        for i in range(0, len(keywords), 5):
            batch = keywords[i : i + 5]
            keyword_groups = [
                {"groupName": kw, "keywords": [kw]}
                for kw in batch
            ]

            try:
                data = self._request_datalab(keyword_groups, start_date, end_date)

                for result in data.get("results", []):
                    title = result.get("title", "")
                    time_series = {}
                    for point in result.get("data", []):
                        period = point.get("period", "")
                        ratio = point.get("ratio", 0.0)
                        time_series[period] = ratio
                    results[title] = time_series

            except Exception as e:
                logger.error(f"Naver DataLab API error for batch {batch}: {e}")

            if i + 5 < len(keywords):
                time.sleep(BATCH_SLEEP_SECONDS)

        return results

    def _parse_trend_data(
        self,
        interest_data: Dict[str, Dict[str, float]],
    ) -> List[TrendData]:
        trend_data_list = []

        for keyword, time_series in interest_data.items():
            for date_str, ratio in time_series.items():
                dt = datetime.strptime(date_str, "%Y-%m-%d")

                trend_data = TrendData(
                    keyword=keyword,
                    source="naver",
                    value=float(ratio),
                    date=dt,
                    metadata={"score": ratio},
                )
                trend_data_list.append(trend_data)

        return trend_data_list

    def crawl_and_save(
        self,
        keywords: List[str],
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> int:
        """크롤링 후 ES에 저장"""
        interest_data = self.fetch_interest_over_time(keywords, start_date, end_date)

        trend_data_list = self._parse_trend_data(interest_data)

        if not trend_data_list:
            return 0

        documents = [
            {
                **data.model_dump(),
                "date": data.date.isoformat(),
            }
            for data in trend_data_list
        ]

        success, errors = es_client.bulk_index(
            index=ESIndex.TREND_DATA,
            documents=documents,
        )
        if errors:
            logger.error(f"Bulk index errors for naver trend data: {errors}")

        return success

    def _normalize_product_name_for_search(self, name: str) -> str:
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
        if not category2:
            return None
        return CATEGORY2_MAPPING.get(str(category2).zfill(2))

    def fetch_product_trends(
        self,
        products: List[Dict[str, Any]],
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> Tuple[Dict[str, Dict[str, float]], List[Dict]]:
        """상품명 트렌드와 카테고리 트렌드를 독립적으로 수집"""
        logger.info(f"[Naver] Fetching trends for {len(products)} products")

        product_keywords: Dict[str, List[int]] = {}
        product_category_map: Dict[int, str] = {}
        product_info_map: Dict[int, Dict] = {}

        for p in products:
            raw_product_id = p.get("product_id") or p.get("id")
            product_name = p.get("product_name") or p.get("name")
            category2 = p.get("category2")

            if not raw_product_id or not product_name:
                continue

            try:
                product_id = int(raw_product_id)
            except (ValueError, TypeError):
                continue

            product_info_map[product_id] = p

            normalized = self._normalize_product_name_for_search(product_name)
            if normalized:
                if normalized not in product_keywords:
                    product_keywords[normalized] = []
                product_keywords[normalized].append(product_id)

            category_name = self._get_category_name(category2)
            if category_name:
                product_category_map[product_id] = category_name

        keywords = list(product_keywords.keys())
        logger.info(f"[Naver] Unique product name keywords: {len(keywords)}")

        name_interest = self.fetch_interest_over_time(
            keywords=keywords,
            start_date=start_date,
            end_date=end_date,
        )

        category_keywords = list(set(product_category_map.values()))
        logger.info(f"[Naver] Unique category keywords: {len(category_keywords)}")

        category_interest: Dict[str, Dict[str, float]] = {}
        if category_keywords:
            category_interest = self.fetch_interest_over_time(
                keywords=category_keywords,
                start_date=start_date,
                end_date=end_date,
            )

        interest_data = {**name_interest, **category_interest}

        mapping_list = []

        for keyword, product_ids in product_keywords.items():
            avg_value = 0.0
            if keyword in name_interest:
                values = list(name_interest[keyword].values())
                avg_value = sum(values) / len(values) if values else 0

            for pid in product_ids:
                mapping_list.append({
                    "product_id": pid,
                    "product_name": (product_info_map[pid].get("product_name")
                                     or product_info_map[pid].get("name")),
                    "keyword": keyword,
                    "keyword_source": "product_name",
                    "trend_source": "naver",
                    "avg_trend_value": avg_value,
                    "timestamp": datetime.now().isoformat(),
                })

        for pid, category_name in product_category_map.items():
            avg_value = 0.0
            if category_name in category_interest:
                values = list(category_interest[category_name].values())
                avg_value = sum(values) / len(values) if values else 0

            if pid not in product_info_map:
                continue
            mapping_list.append({
                "product_id": pid,
                "product_name": (product_info_map[pid].get("product_name")
                                 or product_info_map[pid].get("name")),
                "keyword": category_name,
                "keyword_source": "category",
                "trend_source": "naver",
                "avg_trend_value": avg_value,
                "timestamp": datetime.now().isoformat(),
            })

        return interest_data, mapping_list

    def crawl_product_trends_and_save(
        self,
        top_n: int = 100,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        products: Optional[List[Dict[str, Any]]] = None,
    ) -> Dict[str, Any]:
        """상품명 기반 네이버 트렌드 크롤링 후 ES에 저장"""
        logger.info(f"[Naver] Starting product-based trend crawling")

        if products is None:
            from src.crawlers.google_trends import google_crawler
            products = google_crawler.get_top_products_from_es(top_n=top_n)
        if not products:
            logger.warning("No products found in ES")
            return {
                "success": False,
                "trend_count": 0,
                "mapping_count": 0,
                "message": "No products found in ES",
            }

        interest_data, mapping_list = self.fetch_product_trends(
            products=products,
            start_date=start_date,
            end_date=end_date,
        )

        trend_data_list = self._parse_trend_data(interest_data)
        trend_count = 0
        if trend_data_list:
            documents = [
                {
                    **data.model_dump(),
                    "date": data.date.isoformat(),
                }
                for data in trend_data_list
            ]
            trend_count, trend_errors = es_client.bulk_index(
                index=ESIndex.TREND_DATA,
                documents=documents,
            )
            if trend_errors:
                logger.error(f"Bulk index errors for naver trend data: {trend_errors}")

        mapping_count = 0
        if mapping_list:
            mapping_count, mapping_errors = es_client.bulk_index(
                index=ESIndex.TREND_PRODUCT_MAPPING,
                documents=mapping_list,
            )
            if mapping_errors:
                logger.error(f"Bulk index errors for naver mapping data: {mapping_errors}")

        name_count = sum(1 for m in mapping_list if m["keyword_source"] == "product_name")
        cat_count = sum(1 for m in mapping_list if m["keyword_source"] == "category")
        logger.info(f"[Naver] Saved {trend_count} trends, {mapping_count} mappings "
                     f"(product_name: {name_count}, category: {cat_count})")

        return {
            "success": True,
            "trend_count": trend_count,
            "mapping_count": mapping_count,
            "products_processed": len(products),
            "unique_keywords": len(interest_data),
            "message": f"[Naver] 트렌드 {trend_count}건, 매핑 {mapping_count}건 저장",
        }


naver_crawler = NaverTrendsCrawler()
