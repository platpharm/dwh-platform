"""의약품 데이터 전처리 프로세서"""
import logging
import re
from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd

from shared.clients.pg_client import pg_client

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


class ProductProcessor:
    """상품 데이터 전처리 클래스"""

    def __init__(self):
        self.pg = pg_client

    def process(self) -> Dict[str, Any]:
        """
        상품 데이터 전처리 실행

        Returns:
            처리 결과 딕셔너리
        """
        logger.info("Starting product data preprocessing")
        start_time = datetime.now()

        try:
            # 상품 데이터 조회
            products = self.pg.get_products()

            if not products:
                logger.warning("No product data found")
                return {
                    "success": True,
                    "processed_count": 0,
                    "message": "No product data to process"
                }

            # DataFrame으로 변환
            df = pd.DataFrame(products)
            original_count = len(df)

            # 전처리 수행
            df = self._clean_data(df)
            df = self._enrich_data(df)
            df = self._normalize_data(df)

            processed_count = len(df)
            elapsed = (datetime.now() - start_time).total_seconds()

            logger.info(
                f"Product preprocessing completed: {processed_count} records "
                f"(from {original_count}) in {elapsed:.2f}s"
            )

            return {
                "success": True,
                "processed_count": processed_count,
                "original_count": original_count,
                "elapsed_seconds": elapsed,
                "message": "Product data preprocessing completed successfully"
            }

        except Exception as e:
            logger.error(f"Product preprocessing failed: {str(e)}")
            return {
                "success": False,
                "processed_count": 0,
                "message": f"Product preprocessing failed: {str(e)}"
            }

    def _clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        데이터 정제
        - NULL 값 처리
        - 텍스트 정규화
        """
        logger.info("Cleaning product data")

        # 필수 필드 NULL 제거
        df = df.dropna(subset=["id", "name"])

        # 텍스트 필드 정제
        text_columns = ["name", "efficacy", "ingredient"]
        for col in text_columns:
            if col in df.columns:
                df[col] = df[col].fillna("").astype(str).str.strip()

        return df

    def _enrich_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        데이터 보강
        - 카테고리 매핑
        - 도매사 정보 조인
        """
        logger.info("Enriching product data")

        # 카테고리2 코드를 카테고리명으로 매핑
        if "category2" in df.columns:
            df["category2_name"] = df["category2"].apply(
                lambda x: CATEGORY2_MAPPING.get(str(x).zfill(2), "기타")
                if pd.notna(x) else "기타"
            )

        # 도매사 정보 조인
        vendors = self.pg.get_vendors()
        if vendors:
            vendor_df = pd.DataFrame(vendors)
            vendor_df = vendor_df.rename(columns={"id": "vendor_id", "name": "vendor_name"})
            df = df.merge(vendor_df, on="vendor_id", how="left")

        return df

    def _normalize_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        데이터 정규화
        - 상품명 정규화
        - 성분명 정규화
        """
        logger.info("Normalizing product data")

        # 상품명에서 용량/규격 분리
        if "name" in df.columns:
            df["name_normalized"] = df["name"].apply(self._normalize_product_name)

        # 성분명 정규화
        if "ingredient" in df.columns:
            df["ingredient_normalized"] = df["ingredient"].apply(
                self._normalize_ingredient
            )

        return df

    def _normalize_product_name(self, name: str) -> str:
        """상품명 정규화"""
        if not name:
            return ""

        # 괄호 내용 제거
        normalized = re.sub(r"\([^)]*\)", "", name)
        # 숫자+단위 제거 (예: 100mg, 50ml)
        normalized = re.sub(r"\d+\.?\d*\s*(mg|ml|g|L|정|캡슐|포|개)", "", normalized, flags=re.IGNORECASE)
        # 특수문자 제거
        normalized = re.sub(r"[^\w\s가-힣]", "", normalized)
        # 연속 공백 제거
        normalized = re.sub(r"\s+", " ", normalized).strip()

        return normalized

    def _normalize_ingredient(self, ingredient: str) -> str:
        """성분명 정규화"""
        if not ingredient:
            return ""

        # 괄호 내용 제거
        normalized = re.sub(r"\([^)]*\)", "", ingredient)
        # 숫자+단위 제거
        normalized = re.sub(r"\d+\.?\d*\s*(mg|ml|g|%)", "", normalized, flags=re.IGNORECASE)
        # 특수문자 제거 (쉼표는 유지)
        normalized = re.sub(r"[^\w\s가-힣,]", "", normalized)
        # 연속 공백 제거
        normalized = re.sub(r"\s+", " ", normalized).strip()

        return normalized

    def get_products_with_keywords(self) -> List[Dict[str, Any]]:
        """키워드 추출용 상품 데이터 조회"""
        query = """
            SELECT
                p.id,
                p.name,
                p.efficacy,
                p.ingredient,
                p.category2,
                p.std,
                p.thumb_img1,
                v.name as vendor_name
            FROM product p
            LEFT JOIN vendor v ON p.vendor_id = v.id
            WHERE p.deleted_at IS NULL
            ORDER BY p.id
        """
        return self.pg.fetch_all(query)

    def get_category_mapping(self) -> Dict[str, str]:
        """카테고리2 코드 매핑 반환"""
        return CATEGORY2_MAPPING.copy()


# 싱글톤 인스턴스
product_processor = ProductProcessor()
