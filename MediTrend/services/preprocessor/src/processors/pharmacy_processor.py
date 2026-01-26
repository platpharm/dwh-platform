"""약국 데이터 전처리 프로세서"""
import logging
import re
from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd

from shared.clients.pg_client import pg_client

logger = logging.getLogger(__name__)


class PharmacyProcessor:
    """약국(account) 데이터 전처리 클래스"""

    def __init__(self):
        self.pg = pg_client

    def process(self) -> Dict[str, Any]:
        """
        약국 데이터 전처리 실행

        Returns:
            처리 결과 딕셔너리
        """
        logger.info("Starting pharmacy (account) data preprocessing")
        start_time = datetime.now()

        try:
            # 약국 데이터 조회
            accounts = self.pg.get_accounts()

            if not accounts:
                logger.warning("No account data found")
                return {
                    "success": True,
                    "processed_count": 0,
                    "message": "No account data to process"
                }

            # DataFrame으로 변환
            df = pd.DataFrame(accounts)
            original_count = len(df)

            # 전처리 수행
            df = self._clean_data(df)
            df = self._enrich_data(df)
            df = self._validate_coordinates(df)

            processed_count = len(df)
            elapsed = (datetime.now() - start_time).total_seconds()

            logger.info(
                f"Pharmacy preprocessing completed: {processed_count} records "
                f"(from {original_count}) in {elapsed:.2f}s"
            )

            return {
                "success": True,
                "processed_count": processed_count,
                "original_count": original_count,
                "elapsed_seconds": elapsed,
                "message": "Pharmacy (account) data preprocessing completed successfully"
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

        # 필수 필드 NULL 제거
        df = df.dropna(subset=["id", "name"])

        # 텍스트 필드 정제
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
            # 시/도 추출
            df["region_sido"] = df["address"].apply(self._extract_sido)
            # 시/군/구 추출
            df["region_sigungu"] = df["address"].apply(self._extract_sigungu)

        return df

    def _validate_coordinates(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        좌표 데이터 검증
        - 위경도 범위 확인 (한국 범위)
        """
        logger.info("Validating coordinates")

        # 한국 위경도 범위
        korea_lat_range = (33.0, 43.0)
        korea_lng_range = (124.0, 132.0)

        if "lat" in df.columns and "lng" in df.columns:
            # 유효하지 않은 좌표 마킹
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

    def _normalize_phone(self, phone: str) -> str:
        """전화번호 정규화"""
        if not phone:
            return ""

        # 숫자만 추출
        digits = re.sub(r"\D", "", phone)

        # 형식에 맞게 포맷팅
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

        # 시/도 패턴
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
                # 정규화
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

        # 시/군/구 패턴
        pattern = r"(?:시|도)\s*([가-힣]+(?:시|군|구))"
        match = re.search(pattern, address)
        if match:
            return match.group(1)

        # 특별시/광역시의 구 패턴
        pattern = r"(?:서울|부산|대구|인천|광주|대전|울산)(?:특별시|광역시)?\s*([가-힣]+구)"
        match = re.search(pattern, address)
        if match:
            return match.group(1)

        return ""

    def get_pharmacies_with_stats(self) -> List[Dict[str, Any]]:
        """통계 정보가 포함된 약국 데이터 조회"""
        query = """
            SELECT
                a.id,
                a.name,
                a.address,
                a.lat,
                a.lng,
                a.phone,
                COUNT(DISTINCT o.id) as order_count,
                COUNT(DISTINCT od.product_id) as product_count,
                COALESCE(SUM(od.qty), 0) as total_qty
            FROM account a
            LEFT JOIN orders o ON a.id = o.account_id AND o.deleted_at IS NULL
            LEFT JOIN orders_detail od ON o.id = od.order_id AND od.deleted_at IS NULL
            WHERE a.deleted_at IS NULL
            GROUP BY a.id, a.name, a.address, a.lat, a.lng, a.phone
            ORDER BY order_count DESC
        """
        return self.pg.fetch_all(query)

    def get_pharmacies_by_region(self, sido: Optional[str] = None) -> List[Dict[str, Any]]:
        """지역별 약국 조회"""
        accounts = self.pg.get_accounts()
        df = pd.DataFrame(accounts)

        if df.empty:
            return []

        df = self._clean_data(df)
        df = self._enrich_data(df)

        if sido:
            df = df[df["region_sido"] == sido]

        return df.to_dict("records")


# 싱글톤 인스턴스
pharmacy_processor = PharmacyProcessor()
