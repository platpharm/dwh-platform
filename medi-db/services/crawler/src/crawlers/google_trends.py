
import logging
import re
import time
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Tuple

import pandas as pd
from pytrends.request import TrendReq
from tenacity import retry, stop_after_attempt, wait_exponential

from shared.clients.es_client import es_client
from shared.models.schemas import TrendData
from shared.config import ESIndex

logger = logging.getLogger(__name__)

BATCH_SLEEP_SECONDS = 3

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

    def __init__(self, hl: str = "ko", tz: int = 540, geo: str = "KR"):
        self.hl = hl
        self.tz = tz
        self.geo = geo
        self.pytrends = TrendReq(hl=hl, tz=tz)

    def _get_timeframe(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> str:
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
        timeframe = self._get_timeframe(start_date, end_date)
        results = {}

        for i in range(0, len(keywords), 5):
            batch_keywords = keywords[i : i + 5]

            self.pytrends.build_payload(
                kw_list=batch_keywords,
                cat=0,
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
                time.sleep(BATCH_SLEEP_SECONDS)

        return results

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
    )
    def fetch_related_queries(
        self,
        keywords: List[str],
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> Dict[str, Dict[str, Any]]:
        timeframe = self._get_timeframe(start_date, end_date)
        results = {}

        for i in range(0, len(keywords), 5):
            batch_keywords = keywords[i : i + 5]

            self.pytrends.build_payload(
                kw_list=batch_keywords,
                cat=0,
                timeframe=timeframe,
                geo=self.geo,
            )

            related = self.pytrends.related_queries()

            for keyword in batch_keywords:
                if keyword in related:
                    keyword_data = related[keyword]
                    if keyword_data is None:
                        results[keyword] = {"top": [], "rising": []}
                        continue
                    results[keyword] = {
                        "top": (
                            keyword_data["top"].to_dict("records")
                            if isinstance(keyword_data.get("top"), pd.DataFrame) and not keyword_data["top"].empty
                            else []
                        ),
                        "rising": (
                            keyword_data["rising"].to_dict("records")
                            if isinstance(keyword_data.get("rising"), pd.DataFrame) and not keyword_data["rising"].empty
                            else []
                        ),
                    }

            if i + 5 < len(keywords):
                time.sleep(BATCH_SLEEP_SECONDS)

        return results

    def _parse_trend_data(
        self,
        interest_data: Dict[str, Dict],
        related_data: Optional[Dict[str, Dict]] = None,
    ) -> List[TrendData]:
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
        interest_data = self.fetch_interest_over_time(keywords, start_date, end_date)

        related_data = None
        if include_related:
            related_data = self.fetch_related_queries(
                keywords,
                start_date=start_date,
                end_date=end_date,
            )

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

        success, errors = es_client.bulk_index(
            index=ESIndex.TREND_DATA,
            documents=documents,
        )
        if errors:
            logger.error(f"Bulk index errors for trend data: {errors}")

        return success

    def get_top_products_from_es(
        self,
        top_n: int = 100,
    ) -> List[Dict[str, Any]]:
        logger.info(f"Fetching top {top_n} products from CDC ES")

        try:
            query = {"match_all": {}}

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
                raw_id = p.get("id")
                if raw_id is None:
                    continue
                try:
                    product_id = int(raw_id)
                except (ValueError, TypeError):
                    logger.warning(f"Invalid product id from CDC: {raw_id}, skipping")
                    continue
                result.append({
                    "product_id": product_id,
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
    ) -> Tuple[Dict[str, Any], List[Dict]]:
        logger.info(f"Fetching trends for {len(products)} products")

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
        logger.info(f"Unique product name keywords: {len(keywords)}")

        name_interest = self.fetch_interest_over_time(
            keywords=keywords,
            start_date=start_date,
            end_date=end_date,
        )

        category_keywords = list(set(product_category_map.values()))
        logger.info(f"Unique category keywords: {len(category_keywords)}")

        category_interest = {}
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
                    "trend_source": "google",
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
                "trend_source": "google",
                "avg_trend_value": avg_value,
                "timestamp": datetime.now().isoformat(),
            })

        return interest_data, mapping_list

    def crawl_product_trends_and_save(
        self,
        top_n: int = 100,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        include_related: bool = True,
        products: Optional[List[Dict[str, Any]]] = None,
    ) -> Dict[str, Any]:
        logger.info(f"[Google] Starting product-based trend crawling")

        if products is None:
            products = self.get_top_products_from_es(top_n=top_n)
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

        related_data = None
        if include_related and interest_data:
            all_keywords = list(interest_data.keys())
            related_data = self.fetch_related_queries(
                all_keywords,
                start_date=start_date,
                end_date=end_date,
            )

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
            trend_count, trend_errors = es_client.bulk_index(
                index=ESIndex.TREND_DATA,
                documents=documents,
            )
            if trend_errors:
                logger.error(f"Bulk index errors for trend data: {trend_errors}")

        mapping_count = 0
        if mapping_list:
            mapping_count, mapping_errors = es_client.bulk_index(
                index=ESIndex.TREND_PRODUCT_MAPPING,
                documents=mapping_list,
            )
            if mapping_errors:
                logger.error(f"Bulk index errors for mapping data: {mapping_errors}")

        name_count = sum(1 for m in mapping_list if m["keyword_source"] == "product_name")
        cat_count = sum(1 for m in mapping_list if m["keyword_source"] == "category")
        logger.info(f"[Google] Saved {trend_count} trends, {mapping_count} mappings "
                     f"(product_name: {name_count}, category: {cat_count})")

        return {
            "success": True,
            "trend_count": trend_count,
            "mapping_count": mapping_count,
            "products_processed": len(products),
            "unique_keywords": len(interest_data),
            "message": f"[Google] 트렌드 {trend_count}건, 매핑 {mapping_count}건 저장",
        }

google_crawler = GoogleTrendsCrawler()
