"""
행정안전부 주민등록 인구 및 세대현황 수집기

데이터 소스: 행정안전부 주민등록 인구통계
API: https://apis.data.go.kr/1741000/stdgPpltnHhStus/selectStdgPpltnHhStus
공공데이터 PK: 15108071

ES 인덱스: population_stat
"""

import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from crawler.base_collector import BaseCollector
from crawler.config import CONFIG


class PopulationStatCollector(BaseCollector):
    """
    행정안전부 주민등록 인구 및 세대현황 수집기

    전국 시도별로 읍면동 단위 인구 및 세대 통계를 수집합니다.
    """

    BASE_URL = "https://apis.data.go.kr/1741000/stdgPpltnHhStus"
    ENDPOINT = "/selectStdgPpltnHhStus"
    ES_INDEX = "population_stat"
    PAGE_SIZE = 100

    SIDO_CODES = {
        "11": "서울", "26": "부산", "27": "대구", "28": "인천",
        "29": "광주", "30": "대전", "31": "울산", "36": "세종",
        "41": "경기", "42": "강원", "43": "충북", "44": "충남",
        "45": "전북", "46": "전남", "47": "경북", "48": "경남",
        "50": "제주",
    }

    def __init__(self):
        super().__init__(name="PopulationStatCollector")
        self.service_key = CONFIG["data_go_kr"]["service_key"]

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
    ) -> List[Dict[str, Any]]:
        """
        전국 인구/세대 통계 수집

        Args:
            srch_fr_ym: 조회 시작연월 (YYYYMM). None이면 전월
            srch_to_ym: 조회 종료연월 (YYYYMM). None이면 전월

        Returns:
            수집된 인구 통계 리스트
        """
        if srch_fr_ym is None or srch_to_ym is None:
            default_fr, default_to = self._get_default_search_period()
            srch_fr_ym = srch_fr_ym or default_fr
            srch_to_ym = srch_to_ym or default_to

        all_data = []

        self.logger.info(
            f"주민등록 인구통계 수집 시작 (기간: {srch_fr_ym} ~ {srch_to_ym})"
        )

        for sido_cd, sido_nm in self.SIDO_CODES.items():
            self.logger.info(f"[{sido_nm}] 인구통계 수집 시작")

            items = self._collect_by_sido(sido_cd, sido_nm, srch_fr_ym, srch_to_ym)
            all_data.extend(items)

            self.logger.info(f"[{sido_nm}] 인구통계 수집 완료: {len(items)}건")
            time.sleep(0.1)

        self.logger.info(f"전체 인구통계 수집 완료: 총 {len(all_data)}건")
        return all_data

    def _collect_by_sido(
        self, sido_cd: str, sido_nm: str, srch_fr_ym: str, srch_to_ym: str
    ) -> List[Dict[str, Any]]:
        items = []
        page_no = 1
        total_count = None

        while True:
            result = self._request_population(
                sido_cd, srch_fr_ym, srch_to_ym, page_no
            )

            if result is None:
                self.logger.error(
                    f"[{sido_nm}] 페이지 {page_no} 조회 실패, 다음 시도로 이동"
                )
                break

            if total_count is None:
                total_count = result.get("total_count", 0)
                self.logger.info(f"[{sido_nm}] 전체 건수: {total_count}건")

            page_items = result.get("items", [])
            if not page_items:
                break

            items.extend(page_items)

            if len(items) >= total_count:
                break

            page_no += 1
            time.sleep(0.05)

        return items

    def _request_population(
        self, sido_cd: str, srch_fr_ym: str, srch_to_ym: str, page_no: int
    ) -> Optional[Dict[str, Any]]:
        url = f"{self.BASE_URL}{self.ENDPOINT}"

        params = {
            "serviceKey": self.service_key,
            "pageNo": page_no,
            "numOfRows": self.PAGE_SIZE,
            "type": "json",
            "stdgCd": sido_cd,
            "srchFrYm": srch_fr_ym,
            "srchToYm": srch_to_ym,
            "lv": "2",
        }

        response = self._make_request(url, params=params)

        if response is None:
            return None

        return self._parse_json_response_data(response)

    def _parse_json_response_data(self, response) -> Optional[Dict[str, Any]]:
        try:
            data = response.json()

            result_code = (
                data.get("response", {})
                .get("header", {})
                .get("resultCode")
            )
            if result_code and result_code != "00":
                result_msg = (
                    data.get("response", {})
                    .get("header", {})
                    .get("resultMsg")
                )
                self.logger.error(f"API 에러: {result_code} - {result_msg}")
                return None

            body = data.get("response", {}).get("body", {})
            total_count = int(body.get("totalCount", 0))

            raw_items = body.get("items", {})
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
            stdg_cd = str(item.get("stdgCd", ""))
            stdg_nm = item.get("stdgNm", "")
            srch_ym = str(item.get("srchYm", item.get("statsYm", "")))

            parsed = {
                "srch_ym": srch_ym,
                "stdg_cd": stdg_cd,
                "stdg_nm": stdg_nm,
                "tot_nmpr_cnt": self._safe_int(item.get("totNmprCnt")),
                "male_nmpr_cnt": self._safe_int(item.get("maleNmprCnt")),
                "feml_nmpr_cnt": self._safe_int(item.get("femlNmprCnt")),
                "hh_cnt": self._safe_int(item.get("hhCnt")),
                "collected_at": collected_at,
                "_doc_id": f"{srch_ym}_{stdg_cd}",
            }

            return {k: v for k, v in parsed.items() if v is not None}

        except Exception as e:
            self.logger.error(f"인구통계 항목 파싱 오류: {e}")
            return None

    def _safe_int(self, value: Any) -> Optional[int]:
        if value is None or value == "":
            return None
        try:
            return int(str(value).strip().replace(",", ""))
        except (ValueError, TypeError):
            return None

    def run(self) -> Dict[str, int]:
        """
        수집 및 Elasticsearch 저장 실행

        Returns:
            저장 결과 통계
        """
        self.logger.info("PopulationStatCollector 실행 시작")

        data = self.collect()

        if not data:
            self.logger.warning("수집된 인구통계 데이터가 없습니다.")
            return {"success": 0, "failed": 0}

        result = self.save_to_es(
            data=data,
            index_name=self.ES_INDEX,
            id_field="_doc_id"
        )

        self.logger.info(
            f"PopulationStatCollector 실행 완료: "
            f"성공={result['success']}, 실패={result['failed']}"
        )

        return result


if __name__ == "__main__":
    collector = PopulationStatCollector()
    collector.run()
