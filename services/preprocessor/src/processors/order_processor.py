"""PP주문데이터 전처리 프로세서"""
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd

from shared.clients.pg_client import pg_client

logger = logging.getLogger(__name__)


class OrderProcessor:
    """주문 데이터 전처리 클래스"""

    def __init__(self):
        self.pg = pg_client

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
            elapsed = (datetime.now() - start_time).total_seconds()

            logger.info(
                f"Order preprocessing completed: {processed_count} records "
                f"(from {original_count}) in {elapsed:.2f}s"
            )

            return {
                "success": True,
                "processed_count": processed_count,
                "original_count": original_count,
                "elapsed_seconds": elapsed,
                "message": "Order data preprocessing completed successfully"
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
        if "qty" in df.columns:
            df = df[df["qty"] > 0]

        # 가격이 0 이하인 레코드 제거
        if "price" in df.columns:
            df = df[df["price"] > 0]

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
        if "qty" in df.columns and "price" in df.columns:
            df["total_amount"] = df["qty"] * df["price"]

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

    def get_product_sales_summary(self) -> List[Dict[str, Any]]:
        """상품별 판매 요약 조회"""
        query = """
            SELECT
                od.product_id,
                p.name as product_name,
                COUNT(DISTINCT od.order_id) as order_count,
                COUNT(DISTINCT o.account_id) as pharmacy_count,
                SUM(od.qty) as total_qty,
                SUM(od.qty * od.price) as total_amount
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
                SUM(od.qty) as total_qty,
                SUM(od.qty * od.price) as total_amount
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
