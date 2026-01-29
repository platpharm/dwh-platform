import logging
import re
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from shared.clients.es_client import es_client
from shared.config import ESIndex
from shared.models.schemas import TrendProductMapping

logger = logging.getLogger(__name__)

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

STOPWORDS = {
    "정", "캡슐", "시럽", "액", "정제", "연질캡슐", "경질캡슐",
    "필름코팅정", "서방정", "장용정", "츄어블정", "발포정",
    "mg", "ml", "g", "mcg", "iu", "주", "앰플", "바이알",
    "한국", "제약", "약품", "팜", "파마", "메디", "바이오",
}

class KeywordExtractor:

    def __init__(self):
        self.es = es_client

    def extract_keywords(self, product: Dict[str, Any]) -> Dict[str, List[str]]:
        keywords = {
            "name": [],
            "efficacy": [],
            "ingredient": [],
            "category": [],
        }

        if product.get("name"):
            keywords["name"] = self._extract_name_keywords(product["name"])

        if product.get("efficacy"):
            keywords["efficacy"] = self._extract_efficacy_keywords(product["efficacy"])

        if product.get("ingredient"):
            keywords["ingredient"] = self._extract_ingredient_keywords(product["ingredient"])

        if product.get("category2"):
            keywords["category"] = self._extract_category_keywords(product["category2"])

        return keywords

    def _extract_name_keywords(self, name: str) -> List[str]:
        if not name:
            return []

        cleaned = re.sub(r"\([^)]*\)", "", name)
        cleaned = re.sub(r"\d+\.?\d*\s*(mg|ml|g|mcg|iu|정|캡슐|포|개|%)", "", cleaned, flags=re.IGNORECASE)
        cleaned = re.sub(r"[^\w\s가-힣a-zA-Z]", " ", cleaned)
        tokens = cleaned.split()
        keywords = []
        for token in tokens:
            token = token.strip().lower()
            if len(token) >= 2 and token not in STOPWORDS and not token.isdigit():
                keywords.append(token)

        return list(set(keywords))

    def _extract_efficacy_keywords(self, efficacy: str) -> List[str]:
        if not efficacy:
            return []

        parts = re.split(r"[,\n.·]", efficacy)

        keywords = []
        for part in parts:
            cleaned = part.strip()
            if not cleaned:
                continue

            cleaned = re.sub(r"\([^)]*\)", "", cleaned)
            cleaned = cleaned.strip()

            if len(cleaned) >= 2:
                keywords.append(cleaned)

        return list(set(keywords))

    def _extract_ingredient_keywords(self, ingredient: str) -> List[str]:
        if not ingredient:
            return []

        cleaned = re.sub(r"\([^)]*\)", "", ingredient)
        cleaned = re.sub(r"\d+\.?\d*\s*(mg|ml|g|mcg|iu|%)", "", cleaned, flags=re.IGNORECASE)
        parts = re.split(r"[,·]", cleaned)

        keywords = []
        for part in parts:
            token = part.strip()
            if len(token) >= 2:
                keywords.append(token)

        return list(set(keywords))

    def _extract_category_keywords(self, category2: str) -> List[str]:
        if not category2:
            return []

        category_name = CATEGORY2_MAPPING.get(str(category2).zfill(2))

        if category_name:
            return [category_name]

        return []

    def get_trend_data(self, limit: int = 1000) -> List[Dict[str, Any]]:
        try:
            query = {"match_all": {}}
            return self.es.search(
                index=ESIndex.TREND_DATA,
                query=query,
                size=limit
            )
        except Exception as e:
            logger.warning(f"Failed to get trend data: {str(e)}")
            return []

    def get_trend_score_for_product(self, product_name: str, days: int = 30) -> Tuple[float, bool]:
        try:
            normalized_name = self._normalize_product_name_for_trend(product_name)

            query = {
                "bool": {
                    "should": [
                        {"term": {"keyword": product_name}},
                        {"term": {"keyword": normalized_name}},
                        {"match_phrase": {"keyword": normalized_name}}
                    ],
                    "minimum_should_match": 1
                }
            }

            trend_data = self.es.search(
                index=ESIndex.TREND_DATA,
                query=query,
                size=days
            )

            if trend_data:
                values = [d.get("value", 0) for d in trend_data if d.get("value") is not None]
                if values:
                    trend_score = sum(values) / len(values)
                    logger.debug(f"Direct trend match for '{product_name}': {trend_score:.2f} (from {len(values)} records)")
                    return trend_score, True

            return 0.0, False

        except Exception as e:
            logger.warning(f"Failed to get trend score for product '{product_name}': {str(e)}")
            return 0.0, False

    def _normalize_product_name_for_trend(self, name: str) -> str:
        if not name:
            return ""

        normalized = re.sub(r"\([^)]*\)", "", name)
        normalized = re.sub(r"\d+\.?\d*\s*(mg|ml|g|mcg|iu|정|캡슐|포|개|%)", "", normalized, flags=re.IGNORECASE)
        dosage_forms = ["필름코팅정", "연질캡슐", "경질캡슐", "서방정", "장용정", "정제", "캡슐", "시럽", "액"]
        for word in dosage_forms:
            normalized = re.sub(rf"(?<![가-힣]){re.escape(word)}(?![가-힣])", "", normalized)
        normalized = re.sub(r"\s+", " ", normalized).strip()

        return normalized

    def match_trend_with_product(
        self,
        trend_keyword: str,
        trend_score: float,
        product: Dict[str, Any],
        product_keywords: Dict[str, List[str]]
    ) -> Optional[TrendProductMapping]:
        trend_lower = trend_keyword.lower()
        best_match_type = None
        best_match_score = 0.0

        for match_type, keywords in product_keywords.items():
            for keyword in keywords:
                score = self._calculate_match_score(trend_lower, keyword.lower())
                if score > best_match_score:
                    best_match_score = score
                    best_match_type = match_type

        if best_match_score >= 0.5 and best_match_type:
            return TrendProductMapping(
                keyword=trend_keyword,
                product_id=int(product["id"]),
                product_name=product["name"],
                keyword_source=best_match_type,
                match_score=best_match_score,
                trend_score=trend_score,
                timestamp=datetime.now()
            )

        return None

    def _calculate_match_score(self, trend: str, keyword: str) -> float:
        if trend == keyword:
            return 1.0

        if trend in keyword or keyword in trend:
            shorter = min(len(trend), len(keyword))
            longer = max(len(trend), len(keyword))
            return shorter / longer

        common_prefix = 0
        for i in range(min(len(trend), len(keyword))):
            if trend[i] == keyword[i]:
                common_prefix += 1
            else:
                break

        if common_prefix >= 2:
            return common_prefix / max(len(trend), len(keyword))

        return 0.0

    def _get_products_from_cdc(self) -> List[Dict[str, Any]]:
        logger.info("Fetching products from CDC ES...")

        query = {
            "bool": {
                "must_not": [
                    {"exists": {"field": "deleted_at"}}
                ]
            }
        }

        results = []
        for doc in self.es.scroll_search(
            index=ESIndex.CDC_PRODUCT,
            query=query,
            size=1000
        ):
            raw_id = doc.get("id")
            try:
                product_id = int(raw_id) if raw_id is not None else None
            except (ValueError, TypeError):
                logger.warning(f"Skipping product with non-integer id: {raw_id}")
                continue
            results.append({
                "id": product_id,
                "name": doc.get("name", ""),
                "efficacy": doc.get("efficacy") or doc.get("main_efficacy", ""),
                "ingredient": doc.get("ingredient") or doc.get("main_ingredient", ""),
                "category2": doc.get("category2", ""),
                "std": doc.get("std", ""),
                "thumb_img1": doc.get("thumb_img1", ""),
                "vendor_id": doc.get("vendor_id"),
            })

        logger.info(f"Fetched {len(results)} products from CDC ES")
        return results

    def process(self) -> Dict[str, Any]:
        logger.info("Starting keyword extraction and trend mapping")
        start_time = datetime.now()

        try:
            products = self._get_products_from_cdc()
            if not products:
                return {
                    "success": True,
                    "processed_count": 0,
                    "mapping_count": 0,
                    "message": "No product data to process"
                }

            logger.info(f"Processing {len(products)} products for trend mapping")

            trend_data = self.get_trend_data()
            logger.info(f"Loaded {len(trend_data)} trend keywords for fallback matching")

            product_keywords_map = {}
            for product in products:
                keywords = self.extract_keywords(product)
                product_keywords_map[product["id"]] = {
                    "product": product,
                    "keywords": keywords
                }

            mappings = []
            direct_match_count = 0
            fallback_match_count = 0

            for product in products:
                product_id = product["id"]
                product_name = product.get("name", "")

                if not product_name:
                    continue

                trend_score, is_direct_match = self.get_trend_score_for_product(product_name)

                if is_direct_match and trend_score > 0:
                    mapping = TrendProductMapping(
                        product_id=product_id,
                        product_name=product_name,
                        keyword=product_name,
                        keyword_source="direct_match",
                        match_score=1.0,
                        trend_score=trend_score,
                        timestamp=datetime.now()
                    )
                    mappings.append(mapping.model_dump())
                    direct_match_count += 1
                else:
                    best_mapping = None
                    best_trend_score = 0.0

                    for trend in trend_data:
                        trend_keyword = trend.get("keyword", "")
                        trend_score_from_data = trend.get("score") if trend.get("score") is not None else trend.get("value", 0.0)

                        if not trend_keyword:
                            continue

                        mapping = self.match_trend_with_product(
                            trend_keyword,
                            trend_score_from_data,
                            product,
                            product_keywords_map[product_id]["keywords"]
                        )

                        if mapping and mapping.trend_score > best_trend_score:
                            best_mapping = mapping
                            best_trend_score = mapping.trend_score

                    if best_mapping:
                        mappings.append(best_mapping.model_dump())
                        fallback_match_count += 1

            if mappings:
                success_count, errors = self.es.bulk_index(
                    index=ESIndex.TREND_PRODUCT_MAPPING,
                    documents=mappings,
                    id_field=None
                )
                logger.info(f"Indexed {success_count} trend-product mappings to ES")
            else:
                success_count = 0

            elapsed = (datetime.now() - start_time).total_seconds()

            logger.info(
                f"Keyword extraction completed: {len(products)} products, "
                f"{len(mappings)} mappings ({direct_match_count} direct, {fallback_match_count} fallback) "
                f"in {elapsed:.2f}s"
            )

            return {
                "success": True,
                "processed_count": len(products),
                "product_count": len(products),
                "trend_count": len(trend_data),
                "mapping_count": len(mappings),
                "direct_match_count": direct_match_count,
                "fallback_match_count": fallback_match_count,
                "indexed_count": success_count,
                "elapsed_seconds": elapsed,
                "message": "Keyword extraction and trend mapping completed successfully"
            }

        except Exception as e:
            logger.error(f"Keyword extraction failed: {str(e)}")
            return {
                "success": False,
                "processed_count": 0,
                "message": f"Keyword extraction failed: {str(e)}"
            }

    def extract_all_keywords(self) -> Dict[str, Any]:
        logger.info("Extracting keywords from all products (CDC ES)")

        products = self._get_products_from_cdc()
        if not products:
            return {
                "success": True,
                "product_count": 0,
                "keywords": {},
                "message": "No product data"
            }

        all_keywords = {
            "name": set(),
            "efficacy": set(),
            "ingredient": set(),
            "category": set(),
        }

        for product in products:
            keywords = self.extract_keywords(product)
            for ktype, kwords in keywords.items():
                all_keywords[ktype].update(kwords)

        result_keywords = {k: list(v) for k, v in all_keywords.items()}

        return {
            "success": True,
            "product_count": len(products),
            "keywords": result_keywords,
            "keyword_counts": {k: len(v) for k, v in result_keywords.items()},
            "message": "Keyword extraction completed"
        }

keyword_extractor = KeywordExtractor()
