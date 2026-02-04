"""
병원/약국 건축물대장 연계 수집기

병원(hospital_hira), 약국(pharmacy_hira) 데이터의 좌표를 기반으로
Vworld API에서 건물관리번호를 조회한 후 건축물대장 정보를 연계합니다.

필요 API:
    - Vworld 좌표 → 건물정보 API (https://www.vworld.kr/)
    - 국토교통부 건축물대장 API (data.go.kr)

ES 인덱스: building_enriched
"""

import time
from typing import Any, Dict, List, Optional

from crawler.base_collector import BaseCollector
from crawler.collectors.building_ledger import BuildingLedgerCollector
from crawler.config import CONFIG


class BuildingEnricherCollector(BaseCollector):
    """
    병원/약국 건축물대장 연계 수집기

    HIRA 병원/약국 데이터의 좌표(XPos, YPos)를 사용하여
    Vworld API로 건물관리번호를 조회하고, 건축물대장 정보를 연계합니다.
    """

    ES_INDEX = "building_enriched"
    VWORLD_BASE_URL = "https://api.vworld.kr/req/data"

    def __init__(self):
        super().__init__(name="BuildingEnricherCollector")
        self.vworld_api_key = CONFIG.get("vworld", {}).get("api_key", "")
        self.data_go_kr_key = CONFIG["data_go_kr"]["service_key"]
        self.building_collector = BuildingLedgerCollector()

        if not self.vworld_api_key:
            self.logger.warning(
                "VWORLD_API_KEY가 설정되지 않았습니다. "
                ".env 파일에 VWORLD_API_KEY를 추가하세요. "
                "발급: https://www.vworld.kr/"
            )

    def get_building_info_by_coord(
        self,
        x: float,
        y: float,
        buffer: int = 10
    ) -> Optional[Dict[str, Any]]:
        """
        좌표로 건물 정보 조회 (Vworld API)

        Args:
            x: 경도 (XPos)
            y: 위도 (YPos)
            buffer: 검색 반경 (미터)

        Returns:
            건물 정보 또는 None
        """
        if not self.vworld_api_key:
            return None

        url = self.VWORLD_BASE_URL

        params = {
            "service": "data",
            "request": "GetFeature",
            "data": "LT_C_AISENC",
            "key": self.vworld_api_key,
            "format": "json",
            "geomFilter": f"POINT({x} {y})",
            "buffer": buffer,
            "size": 1,
        }

        response = self._make_request(url, params=params)

        if response is None:
            return None

        try:
            data = response.json()
            features = (
                data.get("response", {})
                .get("result", {})
                .get("featureCollection", {})
                .get("features", [])
            )

            if not features:
                return None

            props = features[0].get("properties", {})
            return {
                "bldMngNo": props.get("bd_mgt_sn"),
                "bldNm": props.get("buld_nm"),
                "platPlc": props.get("buld_addr"),
                "sigunguCd": props.get("sig_cd"),
                "bjdongCd": props.get("emd_cd"),
            }

        except Exception as e:
            self.logger.error(f"Vworld 응답 파싱 오류: {e}")
            return None

    def enrich_facility_with_building(
        self,
        facility: Dict[str, Any],
        facility_type: str = "pharmacy"
    ) -> Optional[Dict[str, Any]]:
        """
        병원/약국에 건축물대장 정보 연계

        Args:
            facility: 병원/약국 데이터 (XPos, YPos 필수)
            facility_type: 시설 유형 (pharmacy, hospital)

        Returns:
            건축물대장 정보가 연계된 데이터 또는 None
        """
        x_pos = facility.get("XPos")
        y_pos = facility.get("YPos")

        if not x_pos or not y_pos:
            self.logger.warning(
                f"좌표 없음: {facility.get('yadmNm', 'Unknown')}"
            )
            return None

        building_info = self.get_building_info_by_coord(x_pos, y_pos)

        if not building_info or not building_info.get("bldMngNo"):
            self.logger.debug(
                f"건물 정보 없음: {facility.get('yadmNm', 'Unknown')}"
            )
            return None

        bld_mng_no = building_info["bldMngNo"]
        ledger_info = self.building_collector.get_building_info_by_bld_mng_no(
            bld_mng_no
        )

        enriched = {
            "facility_type": facility_type,
            "facility_ykiho": facility.get("ykiho"),
            "facility_name": facility.get("yadmNm"),
            "facility_addr": facility.get("addr"),
            "facility_x": x_pos,
            "facility_y": y_pos,
            "facility_sido": facility.get("sidoCdNm"),
            "facility_sggu": facility.get("sgguCdNm"),
            "bld_mng_no": bld_mng_no,
            "bld_nm": building_info.get("bldNm"),
        }

        if ledger_info:
            title_info = ledger_info.get("titleInfo", {})
            if title_info:
                enriched.update({
                    "tot_area": title_info.get("totArea"),
                    "arch_area": title_info.get("archArea"),
                    "plat_area": title_info.get("platArea"),
                    "bc_rat": title_info.get("bcRat"),
                    "vl_rat": title_info.get("vlRat"),
                    "main_purps_cd_nm": title_info.get("mainPurpsCdNm"),
                    "strct_cd_nm": title_info.get("strctCdNm"),
                    "grnd_flr_cnt": title_info.get("grndFlrCnt"),
                    "ugrnd_flr_cnt": title_info.get("ugrndFlrCnt"),
                    "use_apr_day": title_info.get("useAprDay"),
                })
            enriched["area_classification"] = ledger_info.get("areaClassification")

        return enriched

    def collect_from_es(
        self,
        facility_type: str = "pharmacy",
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """
        ES에서 병원/약국 데이터를 조회하여 건축물대장 연계

        Args:
            facility_type: 시설 유형 (pharmacy, hospital)
            limit: 조회 건수

        Returns:
            건축물대장 연계된 데이터 리스트
        """
        index_map = {
            "pharmacy": "pharmacy_hira",
            "hospital": "hospital_hira",
        }

        index_name = index_map.get(facility_type)
        if not index_name:
            self.logger.error(f"알 수 없는 시설 유형: {facility_type}")
            return []

        query = {
            "query": {
                "bool": {
                    "must": [
                        {"exists": {"field": "XPos"}},
                        {"exists": {"field": "YPos"}},
                    ]
                }
            },
            "size": limit,
        }

        try:
            from crawler.utils.es_client import es_client

            response = es_client.search(index=index_name, body=query)
            hits = response.get("hits", {}).get("hits", [])

            facilities = [hit["_source"] for hit in hits]
            self.logger.info(
                f"[{facility_type}] ES에서 {len(facilities)}건 조회"
            )

            return facilities

        except Exception as e:
            self.logger.error(f"ES 조회 오류: {e}")
            return []

    def collect(
        self,
        facilities: Optional[List[Dict[str, Any]]] = None,
        facility_type: str = "pharmacy",
        limit: int = 100,
        delay: float = 0.2
    ) -> List[Dict[str, Any]]:
        """
        병원/약국 건축물대장 연계 수집

        Args:
            facilities: 병원/약국 데이터 리스트. None이면 ES에서 조회
            facility_type: 시설 유형 (pharmacy, hospital)
            limit: 조회 건수 (ES 조회시)
            delay: API 호출 간 딜레이

        Returns:
            건축물대장 연계된 데이터 리스트
        """
        if not self.vworld_api_key:
            self.logger.error(
                "VWORLD_API_KEY가 필요합니다. "
                ".env 파일에 VWORLD_API_KEY를 추가하세요."
            )
            return []

        if facilities is None:
            facilities = self.collect_from_es(facility_type, limit)

        if not facilities:
            self.logger.warning("수집할 시설 데이터가 없습니다.")
            return []

        enriched_list = []

        self.logger.info(
            f"건축물대장 연계 시작: {len(facilities)}건"
        )

        for i, facility in enumerate(facilities):
            enriched = self.enrich_facility_with_building(
                facility, facility_type
            )

            if enriched:
                enriched_list.append(enriched)

            if (i + 1) % 50 == 0:
                self.logger.info(
                    f"진행: {i + 1}/{len(facilities)} "
                    f"(성공: {len(enriched_list)}건)"
                )

            time.sleep(delay)

        self.logger.info(
            f"건축물대장 연계 완료: {len(enriched_list)}건"
        )

        return enriched_list

    def run(
        self,
        facility_type: str = "pharmacy",
        limit: int = 100,
        save_to_es: bool = True
    ) -> Dict[str, int]:
        """
        수집 및 Elasticsearch 저장 실행

        Args:
            facility_type: 시설 유형 (pharmacy, hospital, all)
            limit: 조회 건수
            save_to_es: ES 저장 여부

        Returns:
            저장 결과 통계
        """
        self.logger.info("BuildingEnricherCollector 실행 시작")

        all_data = []

        if facility_type == "all":
            for ft in ["pharmacy", "hospital"]:
                data = self.collect(facility_type=ft, limit=limit)
                all_data.extend(data)
        else:
            all_data = self.collect(facility_type=facility_type, limit=limit)

        if not all_data:
            self.logger.warning("수집된 데이터가 없습니다.")
            return {"success": 0, "failed": 0}

        if save_to_es:
            result = self.save_to_es(
                data=all_data,
                index_name=self.ES_INDEX,
                id_field="facility_ykiho"
            )

            self.logger.info(
                f"BuildingEnricherCollector 실행 완료: "
                f"성공={result['success']}, 실패={result['failed']}"
            )

            return result

        return {"success": len(all_data), "failed": 0}


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="병원/약국 건축물대장 연계 수집기"
    )
    parser.add_argument(
        "--type",
        type=str,
        choices=["pharmacy", "hospital", "all"],
        default="pharmacy",
        help="시설 유형"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=100,
        help="조회 건수"
    )
    parser.add_argument(
        "--no-save",
        action="store_true",
        help="ES 저장 안함"
    )

    args = parser.parse_args()

    collector = BuildingEnricherCollector()
    collector.run(
        facility_type=args.type,
        limit=args.limit,
        save_to_es=not args.no_save
    )
