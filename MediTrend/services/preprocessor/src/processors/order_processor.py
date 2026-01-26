"""PP주문데이터 전처리 프로세서"""
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd

from shared.clients.pg_client import pg_client
from shared.clients.es_client import es_client
from shared.config import ESIndex

logger = logging.getLogger(__name__)


class OrderProcessor:
    """주문 데이터 전처리 클래스"""

    def __init__(self):
        self.pg = pg_client
        self.es = es_client

    def process(self, limit: Optional[int] = None) -> Dict[str, Any]:
        """
        주문 데이터 전처리 실행

        Args:
            limit: 처리할 최대 레코드 수 (None이면 전체)

        Returns:
            처리 결과 딕셔너리
        """
        logger.info("Starting order data preprocessing")
        start_time = datetime.now()

        try:
            # 주문 상세 데이터 조회
            orders_detail = self.pg.get_orders_detail(limit=limit)

            if not orders_detail:
                logger.warning("No order data found")
                return {
                    "success": True,
                    "processed_count": 0,
                    "message": "No order data to process"
                }

            # DataFrame으로 변환
            df = pd.DataFrame(orders_detail)
            original_count = len(df)

            # 전처리 수행
            df = self._clean_data(df)
            df = self._enrich_data(df)
            df = self._aggregate_data(df)

            processed_count = len(df)

            # ES에 인덱싱
            indexed_count = self._index_to_es(df)

            elapsed = (datetime.now() - start_time).total_seconds()

            logger.info(
                f"Order preprocessing completed: {processed_count} records "
                f"(from {original_count}), indexed {indexed_count} to ES in {elapsed:.2f}s"
            )

            return {
                "success": True,
                "processed_count": processed_count,
                "indexed_count": indexed_count,
                "original_count": original_count,
                "elapsed_seconds": elapsed,
                "message": f"Order data preprocessing completed. Indexed {indexed_count} records to ES."
            }

        except Exception as e:
            logger.error(f"Order preprocessing failed: {str(e)}")
            return {
                "success": False,
                "processed_count": 0,
                "message": f"Order preprocessing failed: {str(e)}"
            }

    def _clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        데이터 정제
        - NULL 값 처리
        - 이상치 제거
        - 데이터 타입 변환
        """
        logger.info("Cleaning order data")

        # NULL 값 처리
        df = df.dropna(subset=["order_id", "product_id"])

        # 수량이 0 이하인 레코드 제거
        if "order_qty" in df.columns:
            df = df[df["order_qty"] > 0]

        # 가격이 0 이하인 레코드 제거
        if "order_price" in df.columns:
            df = df[df["order_price"] > 0]

        # 날짜 형식 변환
        if "ordered_at" in df.columns:
            df["ordered_at"] = pd.to_datetime(df["ordered_at"])

        return df

    def _enrich_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        데이터 보강
        - 파생 변수 생성
        - 외부 데이터 조인
        """
        logger.info("Enriching order data")

        # 총 금액 계산
        if "order_qty" in df.columns and "order_price" in df.columns:
            df["total_amount"] = df["order_qty"] * df["order_price"]

        # 주문 날짜 분리
        if "ordered_at" in df.columns:
            df["order_date"] = df["ordered_at"].dt.date
            df["order_year"] = df["ordered_at"].dt.year
            df["order_month"] = df["ordered_at"].dt.month
            df["order_day"] = df["ordered_at"].dt.day
            df["order_weekday"] = df["ordered_at"].dt.dayofweek
            df["order_hour"] = df["ordered_at"].dt.hour

        return df

    def _aggregate_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        데이터 집계
        - 상품별 판매 통계
        - 약국별 구매 통계
        """
        logger.info("Aggregating order data")

        # 원본 데이터 유지 (집계는 별도로 수행)
        return df

    def _serialize_value(self, value):
        """값을 JSON 직렬화 가능한 형태로 변환"""
        if pd.isna(value):
            return None
        if hasattr(value, 'isoformat'):  # datetime, date, Timestamp
            return value.isoformat()
        return value

    def _index_to_es(self, df: pd.DataFrame, batch_size: int = 5000) -> int:
        """
        전처리된 주문 데이터를 ES에 인덱싱

        Args:
            df: 전처리된 DataFrame
            batch_size: 배치 크기

        Returns:
            인덱싱된 문서 수
        """
        logger.info(f"Indexing {len(df)} order records to ES")

        # DataFrame을 dict 리스트로 변환 (JSON 직렬화 가능하도록)
        records = df.copy()

        # 모든 컬럼 처리
        for col in records.columns:
            dtype_str = str(records[col].dtype)

            # datetime64 타입 (timezone aware/naive 모두)
            if 'datetime64' in dtype_str:
                records[col] = records[col].apply(
                    lambda x: x.isoformat() if pd.notna(x) else None
                )
            # object 타입 (Timestamp, date, NaT 등 포함 가능)
            elif dtype_str == 'object':
                records[col] = records[col].apply(self._serialize_value)

        documents = records.to_dict('records')

        # NaN/NaT를 None으로 변환
        for doc in documents:
            for key, value in doc.items():
                if pd.isna(value):
                    doc[key] = None

        # 배치로 나누어 인덱싱
        total_indexed = 0
        for i in range(0, len(documents), batch_size):
            batch = documents[i:i + batch_size]
            try:
                success, errors = self.es.bulk_index(
                    index=ESIndex.PREPROCESSED_ORDER,
                    documents=batch,
                    id_field=None  # 자동 ID 생성
                )
                total_indexed += success
                if errors:
                    logger.warning(f"Batch {i//batch_size + 1}: {len(errors)} documents failed to index")
            except Exception as e:
                logger.error(f"Batch {i//batch_size + 1} indexing failed: {str(e)}")

        logger.info(f"Successfully indexed {total_indexed} order records to ES")
        return total_indexed

    def get_product_sales_summary(self) -> List[Dict[str, Any]]:
        """상품별 판매 요약 조회"""
        query = """
            SELECT
                od.product_id,
                p.name as product_name,
                COUNT(DISTINCT od.order_id) as order_count,
                COUNT(DISTINCT o.account_id) as pharmacy_count,
                SUM(od.order_qty) as total_qty,
                SUM(od.order_qty * od.order_price) as total_amount
            FROM orders_detail od
            JOIN orders o ON od.order_id = o.id
            JOIN product p ON od.product_id = p.id
            WHERE od.deleted_at IS NULL
            GROUP BY od.product_id, p.name
            ORDER BY total_qty DESC
        """
        return self.pg.fetch_all(query)

    def get_pharmacy_purchase_summary(self) -> List[Dict[str, Any]]:
        """약국별 구매 요약 조회"""
        query = """
            SELECT
                o.account_id,
                a.name as pharmacy_name,
                COUNT(DISTINCT o.id) as order_count,
                COUNT(DISTINCT od.product_id) as product_count,
                SUM(od.order_qty) as total_qty,
                SUM(od.order_qty * od.order_price) as total_amount
            FROM orders o
            JOIN orders_detail od ON o.id = od.order_id
            JOIN account a ON o.account_id = a.id
            WHERE od.deleted_at IS NULL
            GROUP BY o.account_id, a.name
            ORDER BY total_amount DESC
        """
        return self.pg.fetch_all(query)


# 싱글톤 인스턴스
order_processor = OrderProcessor()
