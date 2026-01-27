"""약국 데이터 전처리 프로세서"""
import logging
import re
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional
from collections import defaultdict

import pandas as pd
import numpy as np

from shared.clients.es_client import es_client
from shared.config import ESIndex

logger = logging.getLogger(__name__)


class PharmacyProcessor:
    """약국(account) 데이터 전처리 클래스 (CDC ES 데이터 사용)"""

    def __init__(self):
        self.es = es_client

    def _get_accounts_from_cdc(self) -> List[Dict[str, Any]]:
        """CDC ES에서 약국 데이터 조회"""
        logger.info("Fetching accounts from CDC ES...")

        query = {
            "bool": {
                "must": [
                    {"terms": {"role": ["CU", "BH"]}}  # 약국(CU, BH)만 필터링 (대문자)
                ],
                "must_not": [
                    {"exists": {"field": "deleted_at"}}
                ]
            }
        }

        results = []
        for doc in self.es.scroll_search(
            index=ESIndex.CDC_ACCOUNT,
            query=query,
            size=1000
        ):
            # Transform CDC fields to expected format
            address_parts = [
                doc.get("address1", ""),
                doc.get("address2", ""),
                doc.get("address3", "")
            ]
            full_address = " ".join(filter(None, address_parts))

            results.append({
                "id": doc.get("id"),
                "name": doc.get("host_name") or doc.get("name", ""),  # 약국명 우선, 없으면 개인명
                "address": full_address,
                "region": doc.get("region", ""),
                "city": doc.get("city", ""),
                "phone": doc.get("phone_number", ""),
                "host_type": doc.get("host_type", ""),
                "status": doc.get("status", "")
            })

        logger.info(f"Fetched {len(results)} accounts from CDC ES")
        return results

    def _get_order_statistics_from_cdc(self, pharmacy_ids: set, days: int = 90) -> Dict[str, Dict[str, float]]:
        """CDC ES에서 약국별 주문 통계 계산

        Args:
            pharmacy_ids: 약국 ID 집합
            days: 통계 계산 기간 (일)

        Returns:
            약국 ID -> 주문 통계 매핑
        """
        logger.info(f"Calculating order statistics for {len(pharmacy_ids)} pharmacies...")

        account_orders: Dict[str, List[Dict]] = defaultdict(list)

        end_date = datetime.utcnow()
        start_date = end_date - timedelta(days=days)

        query = {
            "bool": {
                "must": [
                    {"range": {"created_at": {"gte": start_date.isoformat(), "lte": end_date.isoformat()}}},
                    {"term": {"status": "OD_CONFIRM"}}
                ],
                "must_not": [
                    {"exists": {"field": "deleted_at"}}
                ]
            }
        }

        order_count = 0
        for doc in self.es.scroll_search(
            index=ESIndex.CDC_ORDERS_DETAIL,
            query=query,
            size=1000
        ):
            account_id = str(doc.get("account_id", ""))
            if account_id and account_id in pharmacy_ids:
                account_orders[account_id].append({
                    "order_id": doc.get("order_id"),
                    "product_id": doc.get("product_id"),
                    "order_qty": doc.get("order_qty", 0) or 0,
                    "order_price": doc.get("order_price", 0) or 0,
                    "created_at": doc.get("created_at")
                })
                order_count += 1

        logger.info(f"Retrieved {order_count} orders for pharmacy statistics")

        stats: Dict[str, Dict[str, float]] = {}

        for account_id, orders in account_orders.items():
            if not orders:
                continue

            unique_orders = len(set(o.get("order_id") for o in orders if o.get("order_id")))

            total_amount = sum(
                (o.get("order_qty", 0) or 0) * (o.get("order_price", 0) or 0)
                for o in orders
            )

            avg_order_amount = total_amount / unique_orders if unique_orders > 0 else 0
            order_frequency = unique_orders / days if days > 0 else 0
            unique_products = len(set(o.get("product_id") for o in orders if o.get("product_id")))

            stats[account_id] = {
                "total_orders": unique_orders,
                "total_amount": total_amount,
                "avg_order_amount": avg_order_amount,
                "order_frequency": order_frequency,
                "unique_products": unique_products
            }

        logger.info(f"Calculated statistics for {len(stats)} pharmacies")
        return stats

    def process(self) -> Dict[str, Any]:
        """
        약국 데이터 전처리 실행 (CDC ES 데이터 사용)

        Returns:
            처리 결과 딕셔너리
        """
        logger.info("Starting pharmacy (account) data preprocessing from CDC ES")
        start_time = datetime.now()

        try:
            accounts = self._get_accounts_from_cdc()

            if not accounts:
                logger.warning("No account data found")
                return {
                    "success": True,
                    "processed_count": 0,
                    "message": "No account data to process"
                }

            df = pd.DataFrame(accounts)
            original_count = len(df)

            df = self._clean_data(df)
            df = self._enrich_data(df)
            df = self._validate_coordinates(df)

            pharmacy_ids = {str(id) for id in df["id"].dropna()}
            order_stats = self._get_order_statistics_from_cdc(pharmacy_ids)
            df = self._merge_order_statistics(df, order_stats)

            processed_count = len(df)

            indexed_count = self._index_to_es(df)

            elapsed = (datetime.now() - start_time).total_seconds()

            logger.info(
                f"Pharmacy preprocessing completed: {processed_count} records "
                f"(from {original_count}), indexed {indexed_count} to ES in {elapsed:.2f}s"
            )

            return {
                "success": True,
                "processed_count": processed_count,
                "indexed_count": indexed_count,
                "original_count": original_count,
                "elapsed_seconds": elapsed,
                "message": f"Pharmacy data preprocessing completed. Indexed {indexed_count} records to ES."
            }

        except Exception as e:
            logger.error(f"Pharmacy preprocessing failed: {str(e)}")
            return {
                "success": False,
                "processed_count": 0,
                "message": f"Pharmacy preprocessing failed: {str(e)}"
            }

    def _clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        데이터 정제
        - NULL 값 처리
        - 텍스트 정규화
        """
        logger.info("Cleaning pharmacy data")

        df = df.dropna(subset=["id", "name"])
        if "name" in df.columns:
            df["name"] = df["name"].str.strip()

        if "address" in df.columns:
            df["address"] = df["address"].fillna("").str.strip()

        if "phone" in df.columns:
            df["phone"] = df["phone"].fillna("").str.strip()
            df["phone"] = df["phone"].apply(self._normalize_phone)

        return df

    def _enrich_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        데이터 보강
        - 주소에서 지역 정보 추출
        """
        logger.info("Enriching pharmacy data")

        if "address" in df.columns:
            df["region_sido"] = df["address"].apply(self._extract_sido)
            df["region_sigungu"] = df["address"].apply(self._extract_sigungu)

        return df

    def _validate_coordinates(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        좌표 데이터 검증
        - 위경도 범위 확인 (한국 범위)
        """
        logger.info("Validating coordinates")

        korea_lat_range = (33.0, 43.0)
        korea_lng_range = (124.0, 132.0)

        if "lat" in df.columns and "lng" in df.columns:
            df["has_valid_coords"] = (
                df["lat"].between(*korea_lat_range) &
                df["lng"].between(*korea_lng_range) &
                df["lat"].notna() &
                df["lng"].notna()
            )

            invalid_count = (~df["has_valid_coords"]).sum()
            if invalid_count > 0:
                logger.warning(f"Found {invalid_count} pharmacies with invalid coordinates")

        return df

    def _merge_order_statistics(
        self,
        df: pd.DataFrame,
        order_stats: Dict[str, Dict[str, float]]
    ) -> pd.DataFrame:
        """
        주문 통계를 약국 데이터에 병합

        Args:
            df: 약국 DataFrame
            order_stats: 약국별 주문 통계

        Returns:
            통계가 병합된 DataFrame
        """
        logger.info("Merging order statistics with pharmacy data")

        df["total_orders"] = 0.0
        df["total_amount"] = 0.0
        df["avg_order_amount"] = 0.0
        df["order_frequency"] = 0.0
        df["unique_products"] = 0.0

        stats_count = 0
        for idx, row in df.iterrows():
            pharmacy_id = str(row.get("id", ""))
            if pharmacy_id in order_stats:
                stats = order_stats[pharmacy_id]
                df.at[idx, "total_orders"] = stats.get("total_orders", 0)
                df.at[idx, "total_amount"] = stats.get("total_amount", 0)
                df.at[idx, "avg_order_amount"] = stats.get("avg_order_amount", 0)
                df.at[idx, "order_frequency"] = stats.get("order_frequency", 0)
                df.at[idx, "unique_products"] = stats.get("unique_products", 0)
                stats_count += 1

        logger.info(f"Merged order statistics for {stats_count} pharmacies")
        return df

    def _index_to_es(self, df: pd.DataFrame, batch_size: int = 1000) -> int:
        """
        전처리된 약국 데이터를 ES에 인덱싱

        Args:
            df: 전처리된 DataFrame
            batch_size: 배치 크기

        Returns:
            인덱싱된 문서 수
        """
        logger.info(f"Indexing {len(df)} pharmacy records to ES")

        records = df.copy()

        # pharmacy_id 필드 추가 (클러스터링 서비스 호환)
        if "id" in records.columns:
            records["pharmacy_id"] = records["id"]

        for col in records.columns:
            if records[col].dtype == 'bool':
                records[col] = records[col].astype(object)

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
                    index=ESIndex.PREPROCESSED_PHARMACY,
                    documents=batch,
                    id_field="pharmacy_id"
                )
                total_indexed += success
                if errors:
                    logger.warning(f"Batch {i//batch_size + 1}: {len(errors)} documents failed to index")
            except Exception as e:
                logger.error(f"Batch {i//batch_size + 1} indexing failed: {str(e)}")

        logger.info(f"Successfully indexed {total_indexed} pharmacy records to ES")
        return total_indexed

    def _normalize_phone(self, phone: str) -> str:
        """전화번호 정규화"""
        if not phone:
            return ""

        digits = re.sub(r"\D", "", phone)

        if len(digits) == 11:  # 010-1234-5678
            return f"{digits[:3]}-{digits[3:7]}-{digits[7:]}"
        elif len(digits) == 10:  # 02-1234-5678 or 031-123-5678
            if digits.startswith("02"):
                return f"{digits[:2]}-{digits[2:6]}-{digits[6:]}"
            else:
                return f"{digits[:3]}-{digits[3:6]}-{digits[6:]}"
        elif len(digits) == 9:  # 02-123-5678
            return f"{digits[:2]}-{digits[2:5]}-{digits[5:]}"
        else:
            return phone

    def _extract_sido(self, address: str) -> str:
        """주소에서 시/도 추출"""
        if not address:
            return ""

        sido_patterns = [
            r"^(서울|부산|대구|인천|광주|대전|울산|세종)",
            r"^(경기|강원|충북|충남|전북|전남|경북|경남|제주)",
            r"^(서울특별시|부산광역시|대구광역시|인천광역시|광주광역시|대전광역시|울산광역시|세종특별자치시)",
            r"^(경기도|강원도|충청북도|충청남도|전라북도|전라남도|경상북도|경상남도|제주특별자치도)",
        ]

        for pattern in sido_patterns:
            match = re.search(pattern, address)
            if match:
                sido = match.group(1)
                sido_map = {
                    "서울특별시": "서울", "서울": "서울",
                    "부산광역시": "부산", "부산": "부산",
                    "대구광역시": "대구", "대구": "대구",
                    "인천광역시": "인천", "인천": "인천",
                    "광주광역시": "광주", "광주": "광주",
                    "대전광역시": "대전", "대전": "대전",
                    "울산광역시": "울산", "울산": "울산",
                    "세종특별자치시": "세종", "세종": "세종",
                    "경기도": "경기", "경기": "경기",
                    "강원도": "강원", "강원": "강원",
                    "충청북도": "충북", "충북": "충북",
                    "충청남도": "충남", "충남": "충남",
                    "전라북도": "전북", "전북": "전북",
                    "전라남도": "전남", "전남": "전남",
                    "경상북도": "경북", "경북": "경북",
                    "경상남도": "경남", "경남": "경남",
                    "제주특별자치도": "제주", "제주": "제주",
                }
                return sido_map.get(sido, sido)

        return ""

    def _extract_sigungu(self, address: str) -> str:
        """주소에서 시/군/구 추출"""
        if not address:
            return ""

        pattern = r"(?:시|도)\s*([가-힣]+(?:시|군|구))"
        match = re.search(pattern, address)
        if match:
            return match.group(1)

        pattern = r"(?:서울|부산|대구|인천|광주|대전|울산)(?:특별시|광역시)?\s*([가-힣]+구)"
        match = re.search(pattern, address)
        if match:
            return match.group(1)

        return ""

    def get_pharmacies_with_stats(self) -> List[Dict[str, Any]]:
        """통계 정보가 포함된 약국 데이터 조회 (약국 role: cu, bh만 포함) - CDC ES 사용"""
        logger.info("Fetching pharmacies with stats from CDC ES...")

        accounts = self._get_accounts_from_cdc()

        if not accounts:
            return []

        pharmacy_ids = {str(a["id"]) for a in accounts if a.get("id")}
        order_stats = self._get_order_statistics_from_cdc(pharmacy_ids)

        results = []
        for account in accounts:
            pharmacy_id = str(account.get("id", ""))
            stats = order_stats.get(pharmacy_id, {})

            results.append({
                "id": account.get("id"),
                "name": account.get("name", ""),
                "address": account.get("address", ""),
                "lat": None,  # CDC 데이터에 lat/lng 없으면 None
                "lng": None,
                "phone": account.get("phone", ""),
                "order_count": stats.get("total_orders", 0),
                "product_count": stats.get("unique_products", 0),
                "total_qty": 0  # 별도 집계 필요 시 추가
            })

        results.sort(key=lambda x: x["order_count"], reverse=True)

        logger.info(f"Fetched {len(results)} pharmacies with stats from CDC ES")
        return results

    def get_pharmacies_by_region(self, sido: Optional[str] = None) -> List[Dict[str, Any]]:
        """지역별 약국 조회 - CDC ES 사용"""
        accounts = self._get_accounts_from_cdc()
        df = pd.DataFrame(accounts)

        if df.empty:
            return []

        df = self._clean_data(df)
        df = self._enrich_data(df)

        if sido:
            df = df[df["region_sido"] == sido]

        return df.to_dict("records")


pharmacy_processor = PharmacyProcessor()
