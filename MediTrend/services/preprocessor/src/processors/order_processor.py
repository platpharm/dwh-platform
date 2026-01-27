"""PP주문데이터 전처리 프로세서"""
import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional
from collections import defaultdict

import pandas as pd

from shared.clients.es_client import es_client
from shared.config import ESIndex

logger = logging.getLogger(__name__)


class OrderProcessor:
    """주문 데이터 전처리 클래스 (CDC ES 데이터 사용)"""

    def __init__(self):
        self.es = es_client

    def _get_orders_detail_from_cdc(self, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """CDC ES에서 주문 상세 데이터 조회 (약국 role: cu, bh만 포함)"""
        logger.info("Fetching orders detail from CDC ES...")

        pharmacy_ids = self._get_pharmacy_ids_from_cdc()

        query = {
            "bool": {
                "must": [
                    {"terms": {"account_id": list(pharmacy_ids)}}
                ],
                "must_not": [
                    {"exists": {"field": "deleted_at"}}
                ]
            }
        }

        results = []
        count = 0
        for doc in self.es.scroll_search(
            index=ESIndex.CDC_ORDERS_DETAIL,
            query=query,
            size=1000
        ):
            results.append({
                "id": doc.get("id"),
                "order_id": doc.get("order_id"),
                "product_id": doc.get("product_id"),
                "order_qty": doc.get("order_qty", 0),
                "order_price": doc.get("order_price", 0),
                "account_id": doc.get("account_id"),
                "ordered_at": doc.get("created_at"),
            })
            count += 1
            if limit and count >= limit:
                break

        logger.info(f"Fetched {len(results)} orders detail from CDC ES")
        return results

    def _get_pharmacy_ids_from_cdc(self) -> set:
        """CDC ES에서 약국 ID 목록 조회 (role: CU, BH)"""
        query = {
            "bool": {
                "must": [
                    {"terms": {"role": ["CU", "BH"]}}
                ],
                "must_not": [
                    {"exists": {"field": "deleted_at"}}
                ]
            }
        }

        pharmacy_ids = set()
        for doc in self.es.scroll_search(
            index=ESIndex.CDC_ACCOUNT,
            query=query,
            size=1000
        ):
            if doc.get("id"):
                pharmacy_ids.add(doc["id"])

        logger.info(f"Found {len(pharmacy_ids)} pharmacy IDs from CDC ES")
        return pharmacy_ids

    def process(self, limit: Optional[int] = None) -> Dict[str, Any]:
        """
        주문 데이터 전처리 실행 (CDC ES 데이터 사용)

        Args:
            limit: 처리할 최대 레코드 수 (None이면 전체)

        Returns:
            처리 결과 딕셔너리
        """
        logger.info("Starting order data preprocessing from CDC ES")
        start_time = datetime.now()

        try:
            orders_detail = self._get_orders_detail_from_cdc(limit=limit)

            if not orders_detail:
                logger.warning("No order data found")
                return {
                    "success": True,
                    "processed_count": 0,
                    "message": "No order data to process"
                }

            df = pd.DataFrame(orders_detail)
            original_count = len(df)

            df = self._clean_data(df)
            df = self._enrich_data(df)
            df = self._aggregate_data(df)

            processed_count = len(df)

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

        df = df.dropna(subset=["order_id", "product_id"])

        if "order_qty" in df.columns:
            df = df[df["order_qty"] > 0]

        if "order_price" in df.columns:
            df = df[df["order_price"] > 0]

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

        if "order_qty" in df.columns and "order_price" in df.columns:
            df["total_amount"] = df["order_qty"] * df["order_price"]

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

        records = df.copy()

        for col in records.columns:
            dtype_str = str(records[col].dtype)

            if 'datetime64' in dtype_str:
                records[col] = records[col].apply(
                    lambda x: x.isoformat() if pd.notna(x) else None
                )
            elif dtype_str == 'object':
                records[col] = records[col].apply(self._serialize_value)

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
        """상품별 판매 요약 조회 (약국 role: cu, bh만 포함) - CDC ES 사용"""
        logger.info("Calculating product sales summary from CDC ES...")

        pharmacy_ids = self._get_pharmacy_ids_from_cdc()
        product_names = self._get_product_names_from_cdc()

        product_stats: Dict[str, Dict] = defaultdict(lambda: {
            "product_id": None,
            "product_name": "",
            "order_ids": set(),
            "pharmacy_ids": set(),
            "total_qty": 0,
            "total_amount": 0
        })

        query = {
            "bool": {
                "must": [
                    {"terms": {"account_id": list(pharmacy_ids)}}
                ],
                "must_not": [
                    {"exists": {"field": "deleted_at"}}
                ]
            }
        }

        for doc in self.es.scroll_search(
            index=ESIndex.CDC_ORDERS_DETAIL,
            query=query,
            size=1000
        ):
            product_id = doc.get("product_id")
            if not product_id:
                continue

            stats = product_stats[product_id]
            stats["product_id"] = product_id
            stats["product_name"] = product_names.get(product_id, "")
            if doc.get("order_id"):
                stats["order_ids"].add(doc["order_id"])
            if doc.get("account_id"):
                stats["pharmacy_ids"].add(doc["account_id"])
            stats["total_qty"] += doc.get("order_qty", 0) or 0
            stats["total_amount"] += (doc.get("order_qty", 0) or 0) * (doc.get("order_price", 0) or 0)

        results = []
        for product_id, stats in product_stats.items():
            results.append({
                "product_id": stats["product_id"],
                "product_name": stats["product_name"],
                "order_count": len(stats["order_ids"]),
                "pharmacy_count": len(stats["pharmacy_ids"]),
                "total_qty": stats["total_qty"],
                "total_amount": stats["total_amount"]
            })

        results.sort(key=lambda x: x["total_qty"], reverse=True)

        logger.info(f"Calculated sales summary for {len(results)} products")
        return results

    def _get_product_names_from_cdc(self) -> Dict[str, str]:
        """CDC ES에서 상품 ID -> 이름 매핑 조회"""
        query = {
            "bool": {
                "must_not": [
                    {"exists": {"field": "deleted_at"}}
                ]
            }
        }

        product_names = {}
        for doc in self.es.scroll_search(
            index=ESIndex.CDC_PRODUCT,
            query=query,
            size=1000
        ):
            if doc.get("id"):
                product_names[doc["id"]] = doc.get("name", "")

        return product_names

    def _get_account_names_from_cdc(self, pharmacy_ids: set) -> Dict[str, str]:
        """CDC ES에서 약국 ID -> 이름 매핑 조회"""
        query = {
            "bool": {
                "must": [
                    {"terms": {"role": ["CU", "BH"]}}
                ],
                "must_not": [
                    {"exists": {"field": "deleted_at"}}
                ]
            }
        }

        account_names = {}
        for doc in self.es.scroll_search(
            index=ESIndex.CDC_ACCOUNT,
            query=query,
            size=1000
        ):
            if doc.get("id"):
                account_names[doc["id"]] = doc.get("host_name") or doc.get("name", "")

        return account_names

    def get_pharmacy_purchase_summary(self) -> List[Dict[str, Any]]:
        """약국별 구매 요약 조회 (약국 role: cu, bh만 포함) - CDC ES 사용"""
        logger.info("Calculating pharmacy purchase summary from CDC ES...")

        pharmacy_ids = self._get_pharmacy_ids_from_cdc()
        account_names = self._get_account_names_from_cdc(pharmacy_ids)

        pharmacy_stats: Dict[str, Dict] = defaultdict(lambda: {
            "account_id": None,
            "pharmacy_name": "",
            "order_ids": set(),
            "product_ids": set(),
            "total_qty": 0,
            "total_amount": 0
        })

        query = {
            "bool": {
                "must": [
                    {"terms": {"account_id": list(pharmacy_ids)}}
                ],
                "must_not": [
                    {"exists": {"field": "deleted_at"}}
                ]
            }
        }

        for doc in self.es.scroll_search(
            index=ESIndex.CDC_ORDERS_DETAIL,
            query=query,
            size=1000
        ):
            account_id = doc.get("account_id")
            if not account_id:
                continue

            stats = pharmacy_stats[account_id]
            stats["account_id"] = account_id
            stats["pharmacy_name"] = account_names.get(account_id, "")
            if doc.get("order_id"):
                stats["order_ids"].add(doc["order_id"])
            if doc.get("product_id"):
                stats["product_ids"].add(doc["product_id"])
            stats["total_qty"] += doc.get("order_qty", 0) or 0
            stats["total_amount"] += (doc.get("order_qty", 0) or 0) * (doc.get("order_price", 0) or 0)

        results = []
        for account_id, stats in pharmacy_stats.items():
            results.append({
                "account_id": stats["account_id"],
                "pharmacy_name": stats["pharmacy_name"],
                "order_count": len(stats["order_ids"]),
                "product_count": len(stats["product_ids"]),
                "total_qty": stats["total_qty"],
                "total_amount": stats["total_amount"]
            })

        results.sort(key=lambda x: x["total_amount"], reverse=True)

        logger.info(f"Calculated purchase summary for {len(results)} pharmacies")
        return results


order_processor = OrderProcessor()
