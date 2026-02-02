"""
소상공인시장진흥공단 상권영역 정보 수집기

데이터 소스: 소상공인시장진흥공단 상권정보
공공데이터 PK: 15012005
Base URL: https://apis.data.go.kr/B553077/api/open/sdsc2
엔드포인트: /storeZoneInAdmi (행정구역 단위 상권 조회)

ES 인덱스: commercial_zone
"""

import time
from datetime import datetime
from typing import Any, Dict, List, Optional

from crawler.base_collector import BaseCollector
from crawler.config import CONFIG


class CommercialZoneCollector(BaseCollector):
    """
    소상공인시장진흥공단 상권 영역 정보 수집기

    전국 상권(골목상권, 발달상권, 전통시장, 관광특구)의
    영역 정보를 행정구역 단위로 수집합니다.
    """

    BASE_URL = "https://apis.data.go.kr/B553077/api/open/sdsc2"
    ES_INDEX = "commercial_zone"
    DEFAULT_NUM_OF_ROWS = 1000

    CTPRVN_CODES = {
        "11": "서울", "26": "부산", "27": "대구", "28": "인천",
        "29": "광주", "30": "대전", "31": "울산", "36": "세종",
        "41": "경기", "42": "강원", "43": "충북", "44": "충남",
        "45": "전북", "46": "전남", "47": "경북", "48": "경남",
        "50": "제주",
    }

    ZONE_TYPES = {
        "A": "골목상권",
        "D": "발달상권",
        "R": "전통시장",
        "U": "관광특구",
    }

    def __init__(self):
        super().__init__(name="CommercialZoneCollector")
        self.service_key = CONFIG["data_go_kr"]["service_key"]

        if not self.service_key:
            self.logger.warning(
                "DATA_GO_KR_API_KEY가 설정되지 않았습니다. "
                ".env 파일을 확인하세요."
            )

    def collect(self) -> List[Dict[str, Any]]:
        """
        전국 상권 영역 정보 수집

        Returns:
            수집된 상권 영역 리스트
        """
        all_zones = []
        collected_ids = set()

        self.logger.info("상권 영역 정보 수집 시작")

        for ctprvn_cd, ctprvn_nm in self.CTPRVN_CODES.items():
            self.logger.info(f"[{ctprvn_nm}] 상권 수집 시작")

            zones = self._collect_by_ctprvn(ctprvn_cd, ctprvn_nm)

            new_zones = []
            for zone in zones:
                trar_no = zone.get("trarNo")
                if trar_no and trar_no not in collected_ids:
                    collected_ids.add(trar_no)
                    new_zones.append(zone)

            all_zones.extend(new_zones)

            self.logger.info(
                f"[{ctprvn_nm}] 상권 수집 완료: {len(new_zones)}건"
            )
            time.sleep(0.1)

        self.logger.info(f"전체 상권 수집 완료: 총 {len(all_zones)}건")
        return all_zones

    def _collect_by_ctprvn(
        self, ctprvn_cd: str, ctprvn_nm: str
    ) -> List[Dict[str, Any]]:
        items = []
        page_no = 1

        while True:
            response_data = self._request_zone_list(ctprvn_cd, page_no)

            if response_data is None:
                break

            page_items = self._extract_items(response_data)
            if not page_items:
                break

            for item in page_items:
                normalized = self._normalize_item(item)
                items.append(normalized)

            total_count = self._get_total_count(response_data)
            current_count = page_no * self.DEFAULT_NUM_OF_ROWS

            if current_count >= total_count:
                break

            page_no += 1
            time.sleep(0.05)

        return items

    def _request_zone_list(
        self, ctprvn_cd: str, page_no: int
    ) -> Optional[Dict[str, Any]]:
        url = f"{self.BASE_URL}/storeZoneInAdmi"

        params = {
            "serviceKey": self.service_key,
            "divId": "ctprvnCd",
            "key": ctprvn_cd,
            "pageNo": page_no,
            "numOfRows": self.DEFAULT_NUM_OF_ROWS,
            "type": "json",
        }

        response = self._make_request(url, params=params)

        if response is None:
            return None

        return self._parse_json_response(response)

    def _extract_items(self, response_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        try:
            body = response_data.get("body", {})
            items = body.get("items", [])
            if not isinstance(items, list):
                return []
            return items
        except (KeyError, TypeError):
            return []

    def _get_total_count(self, response_data: Dict[str, Any]) -> int:
        try:
            body = response_data.get("body", {})
            return int(body.get("totalCount", 0))
        except (KeyError, TypeError, ValueError):
            return 0

    def _normalize_item(self, item: Dict[str, Any]) -> Dict[str, Any]:
        collected_at = datetime.now().isoformat()
        trar_type_cd = item.get("trarTypeCd", "")

        return {
            "trarNo": item.get("trarNo"),
            "trarNm": item.get("trarNm"),
            "trarTypeCd": trar_type_cd,
            "trarTypeNm": (
                item.get("trarTypeNm")
                or self.ZONE_TYPES.get(trar_type_cd, "")
            ),
            "ctprvnCd": item.get("ctprvnCd"),
            "ctprvnNm": item.get("ctprvnNm"),
            "signguCd": item.get("signguCd"),
            "signguNm": item.get("signguNm"),
            "adongCd": item.get("adongCd"),
            "adongNm": item.get("adongNm"),
            "centerLon": self._safe_float(item.get("centerLon")),
            "centerLat": self._safe_float(item.get("centerLat")),
            "areaSize": self._safe_float(item.get("areaSize")),
            "coords": item.get("coords"),
            "collected_at": collected_at,
        }

    def _safe_float(self, value: Any) -> Optional[float]:
        if value is None or value == "":
            return None
        try:
            return float(str(value).strip())
        except (ValueError, TypeError):
            return None

    def run(self) -> Dict[str, int]:
        """
        수집 및 Elasticsearch 저장 실행

        Returns:
            저장 결과 통계
        """
        self.logger.info("CommercialZoneCollector 실행 시작")

        data = self.collect()

        if not data:
            self.logger.warning("수집된 상권 영역 데이터가 없습니다.")
            return {"success": 0, "failed": 0}

        result = self.save_to_es(
            data=data,
            index_name=self.ES_INDEX,
            id_field="trarNo"
        )

        self.logger.info(
            f"CommercialZoneCollector 실행 완료: "
            f"성공={result['success']}, 실패={result['failed']}"
        )

        return result


if __name__ == "__main__":
    collector = CommercialZoneCollector()
    collector.run()
