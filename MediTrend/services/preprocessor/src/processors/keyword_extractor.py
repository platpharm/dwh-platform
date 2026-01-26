"""키워드 추출 및 트렌드 매핑 프로세서"""
import logging
import re
from datetime import datetime
from typing import Any, Dict, List, Optional, Set

from shared.clients.pg_client import pg_client
from shared.clients.es_client import es_client
from shared.config import ESIndex
from shared.models.schemas import TrendProductMapping

logger = logging.getLogger(__name__)


# 카테고리2 코드 매핑
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

# 불용어 리스트 (상품명에서 제거할 단어들)
STOPWORDS = {
    "정", "캡슐", "시럽", "액", "정제", "연질캡슐", "경질캡슐",
    "필름코팅정", "서방정", "장용정", "츄어블정", "발포정",
    "mg", "ml", "g", "mcg", "iu", "주", "앰플", "바이알",
    "한국", "제약", "약품", "팜", "파마", "메디", "바이오",
}


class KeywordExtractor:
    """키워드 추출 및 트렌드 매핑 클래스"""

    def __init__(self):
        self.pg = pg_client
        self.es = es_client

    def extract_keywords(self, product: Dict[str, Any]) -> Dict[str, List[str]]:
        """
        상품에서 키워드 추출

        Args:
            product: 상품 데이터 딕셔너리

        Returns:
            키워드 타입별 리스트
        """
        keywords = {
            "name": [],
            "efficacy": [],
            "ingredient": [],
            "category": [],
        }

        # 상품명에서 키워드 추출
        if product.get("name"):
            keywords["name"] = self._extract_name_keywords(product["name"])

        # 효능에서 키워드 추출
        if product.get("efficacy"):
            keywords["efficacy"] = self._extract_efficacy_keywords(product["efficacy"])

        # 성분에서 키워드 추출
        if product.get("ingredient"):
            keywords["ingredient"] = self._extract_ingredient_keywords(product["ingredient"])

        # 카테고리 키워드 추출
        if product.get("category2"):
            keywords["category"] = self._extract_category_keywords(product["category2"])

        return keywords

    def _extract_name_keywords(self, name: str) -> List[str]:
        """상품명에서 핵심 키워드 추출"""
        if not name:
            return []

        # 괄호 내용 제거
        cleaned = re.sub(r"\([^)]*\)", "", name)

        # 숫자+단위 제거
        cleaned = re.sub(r"\d+\.?\d*\s*(mg|ml|g|mcg|iu|정|캡슐|포|개|%)", "", cleaned, flags=re.IGNORECASE)

        # 특수문자 제거 (한글, 영문만 유지)
        cleaned = re.sub(r"[^\w\s가-힣a-zA-Z]", " ", cleaned)

        # 토큰화
        tokens = cleaned.split()

        # 불용어 및 짧은 단어 제거
        keywords = []
        for token in tokens:
            token = token.strip().lower()
            if len(token) >= 2 and token not in STOPWORDS and not token.isdigit():
                keywords.append(token)

        return list(set(keywords))

    def _extract_efficacy_keywords(self, efficacy: str) -> List[str]:
        """효능에서 키워드 추출"""
        if not efficacy:
            return []

        # 쉼표, 줄바꿈, 마침표로 분리
        parts = re.split(r"[,\n.·]", efficacy)

        keywords = []
        for part in parts:
            # 정제
            cleaned = part.strip()
            if not cleaned:
                continue

            # 괄호 내용 제거
            cleaned = re.sub(r"\([^)]*\)", "", cleaned)
            cleaned = cleaned.strip()

            if len(cleaned) >= 2:
                keywords.append(cleaned)

        return list(set(keywords))

    def _extract_ingredient_keywords(self, ingredient: str) -> List[str]:
        """성분에서 키워드 추출"""
        if not ingredient:
            return []

        # 괄호 내용 제거
        cleaned = re.sub(r"\([^)]*\)", "", ingredient)

        # 숫자+단위 제거
        cleaned = re.sub(r"\d+\.?\d*\s*(mg|ml|g|mcg|iu|%)", "", cleaned, flags=re.IGNORECASE)

        # 쉼표로 분리
        parts = re.split(r"[,·]", cleaned)

        keywords = []
        for part in parts:
            token = part.strip()
            if len(token) >= 2:
                keywords.append(token)

        return list(set(keywords))

    def _extract_category_keywords(self, category2: str) -> List[str]:
        """카테고리 코드에서 키워드 추출"""
        if not category2:
            return []

        # 코드를 카테고리명으로 매핑
        category_name = CATEGORY2_MAPPING.get(str(category2).zfill(2))

        if category_name:
            return [category_name]

        return []

    def get_trend_data(self, limit: int = 1000) -> List[Dict[str, Any]]:
        """ES에서 트렌드 데이터 조회"""
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

    def match_trend_with_product(
        self,
        trend_keyword: str,
        trend_score: float,
        product: Dict[str, Any],
        product_keywords: Dict[str, List[str]]
    ) -> Optional[TrendProductMapping]:
        """
        트렌드 키워드와 상품 키워드 매칭

        Args:
            trend_keyword: 트렌드 키워드
            trend_score: 트렌드 점수
            product: 상품 데이터
            product_keywords: 추출된 상품 키워드

        Returns:
            매칭 결과 또는 None
        """
        trend_lower = trend_keyword.lower()
        best_match_type = None
        best_match_score = 0.0

        # 키워드 타입별 매칭
        for match_type, keywords in product_keywords.items():
            for keyword in keywords:
                score = self._calculate_match_score(trend_lower, keyword.lower())
                if score > best_match_score:
                    best_match_score = score
                    best_match_type = match_type

        # 매칭 임계값 확인 (0.5 이상일 때만 매칭)
        if best_match_score >= 0.5 and best_match_type:
            return TrendProductMapping(
                keyword=trend_keyword,
                product_id=product["id"],
                product_name=product["name"],
                keyword_source=best_match_type,
                match_score=best_match_score,
                trend_score=trend_score,
                timestamp=datetime.now()
            )

        return None

    def _calculate_match_score(self, trend: str, keyword: str) -> float:
        """매칭 점수 계산"""
        # 완전 일치
        if trend == keyword:
            return 1.0

        # 포함 관계
        if trend in keyword or keyword in trend:
            shorter = min(len(trend), len(keyword))
            longer = max(len(trend), len(keyword))
            return shorter / longer

        # 부분 일치 (첫 2글자 이상)
        common_prefix = 0
        for i in range(min(len(trend), len(keyword))):
            if trend[i] == keyword[i]:
                common_prefix += 1
            else:
                break

        if common_prefix >= 2:
            return common_prefix / max(len(trend), len(keyword))

        return 0.0

    def process(self) -> Dict[str, Any]:
        """
        키워드 추출 및 트렌드 매핑 실행

        Returns:
            처리 결과 딕셔너리
        """
        logger.info("Starting keyword extraction and trend mapping")
        start_time = datetime.now()

        try:
            # 상품 데이터 조회
            products = self.pg.get_products()
            if not products:
                return {
                    "success": True,
                    "processed_count": 0,
                    "message": "No product data to process"
                }

            # 트렌드 데이터 조회
            trend_data = self.get_trend_data()
            logger.info(f"Loaded {len(trend_data)} trend keywords")

            # 상품별 키워드 추출
            product_keywords_map = {}
            for product in products:
                keywords = self.extract_keywords(product)
                product_keywords_map[product["id"]] = {
                    "product": product,
                    "keywords": keywords
                }

            # 트렌드-상품 매핑
            mappings = []
            for trend in trend_data:
                trend_keyword = trend.get("keyword", "")
                trend_score = trend.get("score", 0.0)

                if not trend_keyword:
                    continue

                for product_id, data in product_keywords_map.items():
                    mapping = self.match_trend_with_product(
                        trend_keyword,
                        trend_score,
                        data["product"],
                        data["keywords"]
                    )
                    if mapping:
                        mappings.append(mapping.model_dump())

            # ES에 매핑 결과 저장
            if mappings:
                success_count, errors = self.es.bulk_index(
                    index=ESIndex.TREND_PRODUCT_MAPPING,
                    documents=mappings,
                    id_field=None  # 자동 ID 생성
                )
                logger.info(f"Indexed {success_count} trend-product mappings to ES")
            else:
                success_count = 0

            elapsed = (datetime.now() - start_time).total_seconds()

            logger.info(
                f"Keyword extraction completed: {len(products)} products, "
                f"{len(mappings)} mappings in {elapsed:.2f}s"
            )

            return {
                "success": True,
                "product_count": len(products),
                "trend_count": len(trend_data),
                "mapping_count": len(mappings),
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
        """
        모든 상품에서 키워드 추출 (매핑 없이)

        Returns:
            추출된 키워드 통계
        """
        logger.info("Extracting keywords from all products")

        products = self.pg.get_products()
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

        # Set을 List로 변환
        result_keywords = {k: list(v) for k, v in all_keywords.items()}

        return {
            "success": True,
            "product_count": len(products),
            "keywords": result_keywords,
            "keyword_counts": {k: len(v) for k, v in result_keywords.items()},
            "message": "Keyword extraction completed"
        }


# 싱글톤 인스턴스
keyword_extractor = KeywordExtractor()
