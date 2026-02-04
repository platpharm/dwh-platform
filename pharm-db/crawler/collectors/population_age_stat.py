"""
행정안전부 행정동별(통반단위) 성/연령별 주민등록 인구수 수집기

데이터 소스: 행정안전부 주민등록 인구통계
API: https://apis.data.go.kr/1741000/admmSexdAgePpltn/selectAdmmSexdAgePpltn
공공데이터 PK: 15108072

ES 인덱스: population_age_stat

조회 레벨:
    - lv=1: 시도 단위
    - lv=2: 시군구 단위
    - lv=3: 행정동 단위
"""

import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional
from urllib.parse import quote

from crawler.base_collector import BaseCollector
from crawler.config import CONFIG


class PopulationAgeStatCollector(BaseCollector):
    """
    행정안전부 행정동별(통반단위) 성/연령별 주민등록 인구수 수집기

    전국 행정동별로 연령대별 인구 통계를 수집합니다.
    """

    BASE_URL = "https://apis.data.go.kr/1741000/admmSexdAgePpltn"
    ENDPOINT = "/selectAdmmSexdAgePpltn"
    ES_INDEX = "population_age_stat"
    PAGE_SIZE = 100

    SIDO_CODES = {
        "1100000000": "서울특별시",
        "2600000000": "부산광역시",
        "2700000000": "대구광역시",
        "2800000000": "인천광역시",
        "2900000000": "광주광역시",
        "3000000000": "대전광역시",
        "3100000000": "울산광역시",
        "3600000000": "세종특별자치시",
        "4100000000": "경기도",
        "4200000000": "강원특별자치도",
        "4300000000": "충청북도",
        "4400000000": "충청남도",
        "4500000000": "전라북도",
        "4600000000": "전라남도",
        "4700000000": "경상북도",
        "4800000000": "경상남도",
        "5000000000": "제주특별자치도",
    }

    AGE_GROUPS = ["0", "10", "20", "30", "40", "50", "60", "70", "80", "90", "100"]

    def __init__(self):
        super().__init__(name="PopulationAgeStatCollector")
        self.service_key = CONFIG["data_go_kr"]["service_key"]
        self.service_key_encoded = quote(self.service_key, safe="")

        if not self.service_key:
            self.logger.warning(
                "DATA_GO_KR_API_KEY가 설정되지 않았습니다. "
                ".env 파일을 확인하세요."
            )

    def _get_default_search_period(self) -> tuple:
        today = datetime.now()
        first_of_month = today.replace(day=1)
        last_month_end = first_of_month - timedelta(days=1)
        ym = last_month_end.strftime("%Y%m")
        return ym, ym

    def collect(
        self,
        srch_fr_ym: Optional[str] = None,
        srch_to_ym: Optional[str] = None,
        level: str = "dong",
    ) -> List[Dict[str, Any]]:
        """
        전국 연령별 인구통계 수집

        Args:
            srch_fr_ym: 조회 시작연월 (YYYYMM). None이면 전월
            srch_to_ym: 조회 종료연월 (YYYYMM). None이면 전월
            level: 조회 레벨 ("sido", "sigungu", "dong")

        Returns:
            수집된 연령별 인구 통계 리스트
        """
        if srch_fr_ym is None or srch_to_ym is None:
            default_fr, default_to = self._get_default_search_period()
            srch_fr_ym = srch_fr_ym or default_fr
            srch_to_ym = srch_to_ym or default_to

        self.logger.info(
            f"연령별 인구통계 수집 시작 (기간: {srch_fr_ym} ~ {srch_to_ym}, 레벨: {level})"
        )

        if level == "sido":
            return self._collect_sido_level(srch_fr_ym, srch_to_ym)
        elif level == "sigungu":
            return self._collect_sigungu_level(srch_fr_ym, srch_to_ym)
        else:
            return self._collect_dong_level(srch_fr_ym, srch_to_ym)

    def _collect_sido_level(
        self, srch_fr_ym: str, srch_to_ym: str
    ) -> List[Dict[str, Any]]:
        all_data = []

        for sido_cd, sido_nm in self.SIDO_CODES.items():
            self.logger.info(f"[{sido_nm}] 시도 연령별 인구통계 수집")
            items = self._request_and_parse(sido_cd, srch_fr_ym, srch_to_ym, lv="1")
            all_data.extend(items)
            time.sleep(0.05)

        self.logger.info(f"시도 연령별 인구통계 수집 완료: {len(all_data)}건")
        return all_data

    def _collect_sigungu_level(
        self, srch_fr_ym: str, srch_to_ym: str
    ) -> List[Dict[str, Any]]:
        all_data = []

        for sido_cd, sido_nm in self.SIDO_CODES.items():
            self.logger.info(f"[{sido_nm}] 시군구 연령별 인구통계 수집")
            items = self._request_and_parse(sido_cd, srch_fr_ym, srch_to_ym, lv="2")
            all_data.extend(items)
            self.logger.info(f"[{sido_nm}] 시군구 수집 완료: {len(items)}건")
            time.sleep(0.05)

        self.logger.info(f"시군구 연령별 인구통계 수집 완료: {len(all_data)}건")
        return all_data

    def _collect_dong_level(
        self, srch_fr_ym: str, srch_to_ym: str
    ) -> List[Dict[str, Any]]:
        all_data = []

        for sido_cd, sido_nm in self.SIDO_CODES.items():
            self.logger.info(f"[{sido_nm}] 시군구 목록 조회")
            sigungu_list = self._request_and_parse(
                sido_cd, srch_fr_ym, srch_to_ym, lv="2"
            )

            for sigungu in sigungu_list:
                sigungu_cd = sigungu.get("admm_cd")
                sigungu_nm = sigungu.get("sgg_nm", sigungu_cd)

                if not sigungu_cd:
                    continue

                self.logger.info(f"  [{sido_nm} {sigungu_nm}] 행정동 수집")
                dong_items = self._request_and_parse(
                    sigungu_cd, srch_fr_ym, srch_to_ym, lv="3"
                )
                all_data.extend(dong_items)
                time.sleep(0.05)

            self.logger.info(f"[{sido_nm}] 행정동 수집 완료")
            time.sleep(0.1)

        self.logger.info(f"전체 행정동 연령별 인구통계 수집 완료: {len(all_data)}건")
        return all_data

    def _request_and_parse(
        self, admm_cd: str, srch_fr_ym: str, srch_to_ym: str, lv: str
    ) -> List[Dict[str, Any]]:
        items = []
        page_no = 1

        while True:
            result = self._request_population(admm_cd, srch_fr_ym, srch_to_ym, page_no, lv)

            if result is None:
                break

            total_count = result.get("total_count", 0)
            page_items = result.get("items", [])

            if not page_items:
                break

            items.extend(page_items)

            if len(items) >= total_count:
                break

            page_no += 1
            time.sleep(0.03)

        return items

    def _request_population(
        self,
        admm_cd: str,
        srch_fr_ym: str,
        srch_to_ym: str,
        page_no: int,
        lv: str = "1"
    ) -> Optional[Dict[str, Any]]:
        url = (
            f"{self.BASE_URL}{self.ENDPOINT}"
            f"?serviceKey={self.service_key_encoded}"
            f"&admmCd={admm_cd}"
            f"&srchFrYm={srch_fr_ym}"
            f"&srchToYm={srch_to_ym}"
            f"&lv={lv}"
            f"&type=json"
            f"&numOfRows={self.PAGE_SIZE}"
            f"&pageNo={page_no}"
        )

        response = self._make_request(url)

        if response is None:
            return None

        return self._parse_json_response_data(response)

    def _parse_json_response_data(self, response) -> Optional[Dict[str, Any]]:
        try:
            data = response.json()

            head = data.get("Response", {}).get("head", {})
            result_code = head.get("resultCode")

            if result_code and result_code != "0":
                result_msg = head.get("resultMsg")
                if result_code != "3":
                    self.logger.error(f"API 에러: {result_code} - {result_msg}")
                return None

            total_count = int(head.get("totalCount", 0))

            raw_items = data.get("Response", {}).get("items", {})
            if raw_items is None or raw_items == "":
                return {"items": [], "total_count": total_count}

            item_list = raw_items.get("item", [])
            if isinstance(item_list, dict):
                item_list = [item_list]

            items = []
            for item in item_list:
                parsed = self._parse_item(item)
                if parsed:
                    items.append(parsed)

            return {"items": items, "total_count": total_count}

        except Exception as e:
            self.logger.error(f"JSON 파싱 오류: {e}")
            return None

    def _parse_item(self, item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        try:
            collected_at = datetime.now().isoformat()
            admm_cd = str(item.get("admmCd", ""))
            stats_ym = str(item.get("statsYm", ""))

            parsed = {
                "stats_ym": stats_ym,
                "admm_cd": admm_cd,
                "ctpv_nm": item.get("ctpvNm"),
                "sgg_nm": item.get("sggNm"),
                "dong_nm": item.get("dongNm"),
                "tong": item.get("tong"),
                "ban": item.get("ban"),
                "tot_nmpr_cnt": self._safe_int(item.get("totNmprCnt")),
                "male_nmpr_cnt": self._safe_int(item.get("maleNmprCnt")),
                "feml_nmpr_cnt": self._safe_int(item.get("femlNmprCnt")),
                "collected_at": collected_at,
                "_doc_id": f"{stats_ym}_{admm_cd}",
            }

            for age in self.AGE_GROUPS:
                male_key = f"male{age}AgeNmprCnt"
                feml_key = f"feml{age}AgeNmprCnt"
                parsed[f"male_{age}_age"] = self._safe_int(item.get(male_key))
                parsed[f"feml_{age}_age"] = self._safe_int(item.get(feml_key))

            return {k: v for k, v in parsed.items() if v is not None}

        except Exception as e:
            self.logger.error(f"연령별 인구통계 항목 파싱 오류: {e}")
            return None

    def _safe_int(self, value: Any) -> Optional[int]:
        if value is None or value == "":
            return None
        try:
            return int(str(value).strip().replace(",", ""))
        except (ValueError, TypeError):
            return None

    def run(self, level: str = "dong") -> Dict[str, int]:
        """
        수집 및 Elasticsearch 저장 실행

        Args:
            level: 조회 레벨 ("sido", "sigungu", "dong")

        Returns:
            저장 결과 통계
        """
        self.logger.info(f"PopulationAgeStatCollector 실행 시작 (레벨: {level})")

        data = self.collect(level=level)

        if not data:
            self.logger.warning("수집된 연령별 인구통계 데이터가 없습니다.")
            return {"success": 0, "failed": 0}

        result = self.save_to_es(
            data=data,
            index_name=self.ES_INDEX,
            id_field="_doc_id"
        )

        self.logger.info(
            f"PopulationAgeStatCollector 실행 완료: "
            f"성공={result['success']}, 실패={result['failed']}"
        )

        return result


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="연령별 인구통계 수집기")
    parser.add_argument(
        "--level",
        type=str,
        choices=["sido", "sigungu", "dong"],
        default="dong",
        help="조회 레벨 (sido: 시도, sigungu: 시군구, dong: 행정동)"
    )

    args = parser.parse_args()

    collector = PopulationAgeStatCollector()
    collector.run(level=args.level)
