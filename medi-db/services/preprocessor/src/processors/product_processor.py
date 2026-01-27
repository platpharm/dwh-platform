"""의약품 데이터 전처리 프로세서"""
import logging
import re
from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd

from shared.clients.es_client import es_client
from shared.config import ESIndex

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
    """상품 데이터 전처리 클래스 (CDC ES 데이터 사용)"""

    def __init__(self):
        self.es = es_client

    def _get_products_from_cdc(self) -> List[Dict[str, Any]]:
        """CDC ES에서 상품 데이터 조회"""
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
            results.append({
                "id": doc.get("id"),
                "name": doc.get("name", ""),
                "efficacy": doc.get("efficacy") or doc.get("main_efficacy", ""),
                "ingredient": doc.get("ingredient") or doc.get("main_ingredient", ""),
                "category1": doc.get("category1", ""),
                "category2": doc.get("category2", ""),
                "price": doc.get("price") or doc.get("std_price") or 0,
                "product_kind": doc.get("product_kind", ""),
                "mfr_name": doc.get("mfr_name", ""),
                "std": doc.get("std", ""),
                "thumb_img1": doc.get("thumb_img1", ""),
                "vendor_id": doc.get("vendor_id"),
            })

        logger.info(f"Fetched {len(results)} products from CDC ES")
        return results

    def _get_vendors_from_cdc(self) -> List[Dict[str, Any]]:
        """CDC ES에서 도매사 데이터 조회 (account 인덱스에서 vendor role 조회)"""
        logger.info("Fetching vendors from CDC ES...")

        query = {
            "bool": {
                "must": [
                    {"terms": {"role": ["VD", "VENDOR"]}}
                ],
                "must_not": [
                    {"exists": {"field": "deleted_at"}}
                ]
            }
        }

        results = []
        try:
            for doc in self.es.scroll_search(
                index=ESIndex.CDC_ACCOUNT,
                query=query,
                size=1000
            ):
                results.append({
                    "id": doc.get("id"),
                    "name": doc.get("host_name") or doc.get("name", ""),
                })
        except Exception as e:
            logger.warning(f"Failed to fetch vendors from CDC account index: {e}")
            return []

        logger.info(f"Fetched {len(results)} vendors from CDC ES")
        return results

    def process(self) -> Dict[str, Any]:
        """
        상품 데이터 전처리 실행 (CDC ES 데이터 사용)

        Returns:
            처리 결과 딕셔너리
        """
        logger.info("Starting product data preprocessing from CDC ES")
        start_time = datetime.now()

        try:
            # CDC ES에서 상품 데이터 조회
            products = self._get_products_from_cdc()

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

            # 전처리된 데이터를 ES에 인덱싱
            indexed_count = self._index_to_es(df)

            elapsed = (datetime.now() - start_time).total_seconds()

            logger.info(
                f"Product preprocessing completed: {processed_count} records "
                f"(from {original_count}), indexed {indexed_count} to ES in {elapsed:.2f}s"
            )

            return {
                "success": True,
                "processed_count": processed_count,
                "indexed_count": indexed_count,
                "original_count": original_count,
                "elapsed_seconds": elapsed,
                "message": f"Product data preprocessing completed. Indexed {indexed_count} records to ES."
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
        - 도매사 정보 조인 (CDC 인덱스 가용 시)
        """
        logger.info("Enriching product data")

        # 카테고리2 코드를 카테고리명으로 매핑
        if "category2" in df.columns:
            df["category2_name"] = df["category2"].apply(
                lambda x: CATEGORY2_MAPPING.get(str(x).strip().zfill(2), "기타")
                if pd.notna(x) and str(x).strip() != "" else "기타"
            )

        # 도매사 정보 조인 (CDC account 인덱스에서)
        vendors = self._get_vendors_from_cdc()
        if vendors:
            vendor_df = pd.DataFrame(vendors)
            vendor_df = vendor_df.rename(columns={"id": "vendor_id", "name": "vendor_name"})
            df = df.merge(vendor_df, on="vendor_id", how="left")
        else:
            df["vendor_name"] = None

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

    def _index_to_es(self, df: pd.DataFrame, batch_size: int = 1000) -> int:
        """
        전처리된 상품 데이터를 ES에 인덱싱

        Args:
            df: 전처리된 DataFrame
            batch_size: 배치 크기

        Returns:
            인덱싱된 문서 수
        """
        logger.info(f"Indexing {len(df)} product records to ES")

        records = df.copy()

        for col in records.columns:
            dtype_str = str(records[col].dtype)
            if dtype_str == 'object':
                records[col] = records[col].apply(
                    lambda x: None if pd.isna(x) else x
                )

        documents = records.to_dict('records')

        for doc in documents:
            for key, value in doc.items():
                if pd.isna(value):
                    doc[key] = None

        total_indexed = 0
        for i in range(0, len(documents), batch_size):
            batch = documents[i:i + batch_size]
            try:
                success, errors = self.es.bulk_index(
                    index=ESIndex.PREPROCESSED_PRODUCT,
                    documents=batch,
                    id_field="id"
                )
                total_indexed += success
                if errors:
                    logger.warning(f"Batch {i//batch_size + 1}: {len(errors)} documents failed to index")
            except Exception as e:
                logger.error(f"Batch {i//batch_size + 1} indexing failed: {str(e)}")

        logger.info(f"Successfully indexed {total_indexed} product records to ES")
        return total_indexed

    def _normalize_product_name(self, name: str) -> str:
        """상품명 정규화"""
        if not name:
            return ""

        # 괄호 내용 제거
        normalized = re.sub(r"\([^)]*\)", "", name)
        # 숫자+단위 제거 (예: 100mg, 50ml, 500mcg)
        normalized = re.sub(r"\d+\.?\d*\s*(mg|ml|g|L|mcg|iu|정|캡슐|포|개|%)", "", normalized, flags=re.IGNORECASE)
        # 특수문자 제거 (한글, 영문, 숫자, 공백 유지)
        normalized = re.sub(r"[^\w\s가-힣a-zA-Z]", "", normalized)
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
        normalized = re.sub(r"\d+\.?\d*\s*(mg|ml|g|mcg|iu|%)", "", normalized, flags=re.IGNORECASE)
        # 특수문자 제거 (쉼표는 유지, 한글/영문/숫자 유지)
        normalized = re.sub(r"[^\w\s가-힣a-zA-Z,]", "", normalized)
        # 연속 공백 제거
        normalized = re.sub(r"\s+", " ", normalized).strip()

        return normalized

    def get_products_with_keywords(self) -> List[Dict[str, Any]]:
        """키워드 추출용 상품 데이터 조회 (CDC ES에서)"""
        return self._get_products_from_cdc()

    def get_category_mapping(self) -> Dict[str, str]:
        """카테고리2 코드 매핑 반환"""
        return CATEGORY2_MAPPING.copy()


# 싱글톤 인스턴스
product_processor = ProductProcessor()
