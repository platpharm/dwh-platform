"""
Health Statistics Collector Module

보건의료 통계 데이터를 수집합니다.

데이터 소스:
1. 의료기관 시군구별 진료비 통계 (CSV 파일 다운로드) - data.go.kr 15055561
2. 시도별 의료기관종별 진료비 통계 (CSV 파일 다운로드) - data.go.kr 15139381
3. 의약품사용정보 (REST API) - data.go.kr 15047819 (msupUserInfoService1.2)
"""

import csv
import io
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

from crawler.base_collector import BaseCollector
from crawler.config import CONFIG


FILE_URLS = {
    "medical_cost": (
        "https://www.data.go.kr/cmm/cmm/fileDownload.do"
        "?atchFileId=FILE_000000003179263&fileDetailSn=1&insertDataPrcus=N"
    ),
    "medical_type": (
        "https://www.data.go.kr/cmm/cmm/fileDownload.do"
        "?atchFileId=FILE_000000003547916&fileDetailSn=1&insertDataPrcus=N"
    ),
}

DRUG_USAGE_API = {
    "base_url": "https://apis.data.go.kr/B551182/msupUserInfoService1.2",
    "endpoints": {
        "by_efficacy_area": "/getMeftDivAreaList1.2",
        "by_component_area": "/getCmpnAreaList1.2",
        "by_atc3_area": "/getAtcStp3AreaList1.2",
    },
}

SIDO_CODES = {
    "11": "서울", "21": "부산", "22": "대구", "23": "인천",
    "24": "광주", "25": "대전", "26": "울산", "29": "세종",
    "31": "경기", "32": "강원", "33": "충북", "34": "충남",
    "35": "전북", "36": "전남", "37": "경북", "38": "경남",
    "39": "제주",
}

HIRA_SIDO_CODES = {
    "110000": "서울", "210000": "부산", "220000": "대구", "230000": "인천",
    "240000": "광주", "250000": "대전", "260000": "울산", "310000": "경기",
    "320000": "강원", "330000": "충북", "340000": "충남", "350000": "전북",
    "360000": "전남", "370000": "경북", "380000": "경남", "390000": "제주",
    "410000": "세종",
}

CSV_FIELD_MAP = {
    "진료년도": "st_yy",
    "시도": "sido_nm",
    "시군구": "sggu_nm",
    "의료기관종별": "facility_type",
    "환자수": "pat_cnt",
    "명세서청구건수": "rcpt_cnt",
    "입내원일수": "visit_days",
    "보험자부담금(선별포함)": "ins_amt",
    "요양급여비용총액(선별포함)": "total_benefit_amt",
}


class HealthStatCollector(BaseCollector):
    """
    보건의료 통계 수집기

    CSV 파일 다운로드 방식과 REST API 방식을 모두 지원합니다.
    - 의료비통계: data.go.kr CSV 파일 다운로드
    - 약품사용통계: HIRA msupUserInfoService1.2 REST API
    """

    def __init__(self):
        super().__init__(name="HealthStatCollector", index_name="health_stat")
        self.service_key = CONFIG["data_go_kr"]["service_key"]

        if not self.service_key:
            self.logger.warning(
                "DATA_GO_KR_API_KEY 환경 변수가 설정되지 않았습니다."
            )

    def collect(
        self,
        collect_medical_cost: bool = True,
        collect_medical_type: bool = True,
        collect_drug_usage: bool = True,
    ) -> List[Dict[str, Any]]:
        """
        보건의료 통계 데이터 수집

        Args:
            collect_medical_cost: 시군구별 진료비 통계 수집 여부
            collect_medical_type: 의료기관종별 진료비 통계 수집 여부
            collect_drug_usage: 의약품사용정보 수집 여부 (API 구독 필요)

        Returns:
            수집된 통계 데이터 리스트
        """
        all_data = []

        if collect_medical_cost:
            self.logger.info("시군구별 진료비 통계 CSV 다운로드 시작")
            medical_cost_data = self._collect_medical_cost_csv()
            all_data.extend(medical_cost_data)
            self.logger.info(f"시군구별 진료비 통계 수집 완료: {len(medical_cost_data)}건")

        if collect_medical_type:
            self.logger.info("의료기관종별 진료비 통계 CSV 다운로드 시작")
            medical_type_data = self._collect_medical_type_csv()
            all_data.extend(medical_type_data)
            self.logger.info(f"의료기관종별 진료비 통계 수집 완료: {len(medical_type_data)}건")

        if collect_drug_usage:
            self.logger.info("의약품사용정보 API 수집 시작")
            drug_data = self._collect_drug_usage_api()
            all_data.extend(drug_data)
            self.logger.info(f"의약품사용정보 수집 완료: {len(drug_data)}건")

        self.logger.info(f"전체 보건의료 통계 수집 완료: 총 {len(all_data)}건")
        return all_data

    def _download_csv(self, url: str, encoding: str = "euc-kr") -> List[Dict[str, str]]:
        """
        data.go.kr에서 CSV 파일을 다운로드하고 파싱합니다.

        Args:
            url: 다운로드 URL
            encoding: 파일 인코딩 (기본값: euc-kr)

        Returns:
            CSV 행을 딕셔너리 리스트로 반환
        """
        self.logger.info(f"CSV 다운로드: {url[:80]}...")

        response = self._make_request(url=url, timeout=60)
        if response is None:
            self.logger.error("CSV 파일 다운로드 실패")
            return []

        content_type = response.headers.get("Content-Type", "")
        if "text/html" in content_type:
            self.logger.error(
                "CSV 대신 HTML 응답 수신 - 파일 ID가 변경되었거나 로그인 필요"
            )
            return []

        try:
            text = response.content.decode(encoding)
        except UnicodeDecodeError:
            try:
                text = response.content.decode("utf-8")
            except UnicodeDecodeError:
                text = response.content.decode("cp949")

        reader = csv.DictReader(io.StringIO(text))
        rows = list(reader)
        self.logger.info(f"CSV 파싱 완료: {len(rows)}행")
        return rows

    def _collect_medical_cost_csv(self) -> List[Dict[str, Any]]:
        """
        시군구별 진료비 통계 CSV 다운로드 및 파싱

        Returns:
            가공된 의료비 통계 데이터 리스트
        """
        rows = self._download_csv(FILE_URLS["medical_cost"])
        if not rows:
            return []

        collected_at = datetime.now().isoformat()
        results = []

        for row in rows:
            processed = {
                "stat_type": "medical_cost",
                "st_yy": row.get("진료년도", "").strip(),
                "sido_nm": row.get("시도", "").strip(),
                "sggu_nm": row.get("시군구", "").strip(),
                "pat_cnt": self._safe_int(row.get("환자수")),
                "rcpt_cnt": self._safe_int(row.get("명세서청구건수")),
                "visit_days": self._safe_int(row.get("입내원일수")),
                "ins_amt": self._safe_int(row.get("보험자부담금(선별포함)")),
                "total_benefit_amt": self._safe_int(row.get("요양급여비용총액(선별포함)")),
                "collected_at": collected_at,
            }
            processed["_doc_id"] = (
                f"{processed['st_yy']}_{processed['sido_nm']}_"
                f"{processed['sggu_nm']}_medical_cost"
            )
            results.append(processed)

        return results

    def _collect_medical_type_csv(self) -> List[Dict[str, Any]]:
        """
        의료기관종별 진료비 통계 CSV 다운로드 및 파싱

        Returns:
            가공된 의료기관종별 통계 데이터 리스트
        """
        rows = self._download_csv(FILE_URLS["medical_type"])
        if not rows:
            return []

        collected_at = datetime.now().isoformat()
        results = []

        for row in rows:
            processed = {
                "stat_type": "medical_type",
                "st_yy": row.get("진료년도", "").strip(),
                "sido_nm": row.get("시도", "").strip(),
                "facility_type": row.get("의료기관종별", "").strip(),
                "pat_cnt": self._safe_int(row.get("환자수")),
                "rcpt_cnt": self._safe_int(row.get("명세서청구건수")),
                "visit_days": self._safe_int(row.get("입내원일수")),
                "ins_amt": self._safe_int(row.get("보험자부담금(선별포함)")),
                "total_benefit_amt": self._safe_int(row.get("요양급여비용총액(선별포함)")),
                "collected_at": collected_at,
            }
            processed["_doc_id"] = (
                f"{processed['st_yy']}_{processed['sido_nm']}_"
                f"{processed['facility_type']}_medical_type"
            )
            results.append(processed)

        return results

    def _collect_drug_usage_api(
        self,
        diag_ym: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        의약품사용정보 REST API 수집 (msupUserInfoService1.2)

        data.go.kr에서 해당 API 활용 신청이 필요합니다.
        https://www.data.go.kr/data/15047819/openapi.do

        Args:
            diag_ym: 진료년월 (YYYYMM), None이면 전년도 12월

        Returns:
            의약품 사용 통계 데이터 리스트
        """
        if not self.service_key:
            self.logger.error("API 키가 설정되지 않아 약품사용정보 수집을 중단합니다.")
            return []

        if diag_ym is None:
            diag_ym = f"{datetime.now().year - 1}12"

        all_data = []
        base_url = (
            f"{DRUG_USAGE_API['base_url']}"
            f"{DRUG_USAGE_API['endpoints']['by_efficacy_area']}"
        )

        for sido_cd, sido_nm in HIRA_SIDO_CODES.items():
            self.logger.info(f"의약품사용정보 수집 중: {sido_nm}")
            page_no = 1

            while True:
                params = {
                    "ServiceKey": self.service_key,
                    "diagYm": diag_ym,
                    "insupTp": "0",
                    "cpmdPrscTp": "01",
                    "sidoCd": sido_cd,
                    "numOfRows": 100,
                    "pageNo": page_no,
                }

                response = self._make_request(url=base_url, params=params)

                if response is None:
                    self.logger.warning(
                        f"의약품사용정보 요청 실패: {sido_nm}, page={page_no}"
                    )
                    break

                if response.status_code == 403:
                    self.logger.error(
                        "의약품사용정보 API 접근 거부 (403) - "
                        "data.go.kr에서 API 활용 신청이 필요합니다: "
                        "https://www.data.go.kr/data/15047819/openapi.do"
                    )
                    return all_data

                data = self._parse_xml_or_json(response)
                if data is None:
                    break

                items = self._extract_drug_items(data)
                if not items:
                    break

                collected_at = datetime.now().isoformat()
                for item in items:
                    processed = self._process_drug_usage_item(item, collected_at)
                    all_data.append(processed)

                total_count = self._get_total_count_from_response(data)
                if total_count and page_no * 100 >= total_count:
                    break

                page_no += 1
                time.sleep(0.1)

        return all_data

    def _parse_xml_or_json(self, response) -> Optional[Dict]:
        content_type = response.headers.get("Content-Type", "")
        if "json" in content_type:
            return self._parse_json_response(response)

        try:
            import xml.etree.ElementTree as ET
            root = ET.fromstring(response.text)
            return {"_xml_root": root}
        except Exception as e:
            self.logger.error(f"XML 파싱 실패: {e}")
            return None

    def _extract_drug_items(self, data: Dict) -> List[Dict]:
        if "_xml_root" in data:
            root = data["_xml_root"]
            items = []
            for item_el in root.findall(".//item"):
                item_dict = {}
                for child in item_el:
                    item_dict[child.tag] = child.text
                items.append(item_dict)
            return items

        try:
            response_data = data.get("response", {})
            body = response_data.get("body", {})
            items = body.get("items", {})
            if items is None or items == "":
                return []
            item_list = items.get("item", [])
            if isinstance(item_list, dict):
                return [item_list]
            return item_list if item_list else []
        except (AttributeError, TypeError):
            return []

    def _get_total_count_from_response(self, data: Dict) -> Optional[int]:
        if "_xml_root" in data:
            root = data["_xml_root"]
            tc = root.findtext(".//totalCount")
            return int(tc) if tc else None

        try:
            return int(
                data.get("response", {}).get("body", {}).get("totalCount", 0)
            )
        except (AttributeError, TypeError, ValueError):
            return None

    def _process_drug_usage_item(
        self,
        item: Dict[str, Any],
        collected_at: str,
    ) -> Dict[str, Any]:
        processed = {
            "stat_type": "drug_usage",
            "diag_ym": item.get("diagYm"),
            "insup_tp_cd": item.get("insupTpCd"),
            "sido_cd": item.get("sidoCd"),
            "sido_nm": item.get("sidoCdNm"),
            "sggu_cd": item.get("sgguCd"),
            "sggu_nm": item.get("sgguCdNm"),
            "meft_div_no": item.get("meftDivNo"),
            "meft_div_nm": item.get("meftDivNm"),
            "msup_use_amt": self._safe_float(item.get("msupUseAmt")),
            "tot_use_qty": self._safe_float(item.get("totUseQty")),
            "collected_at": collected_at,
        }
        processed["_doc_id"] = (
            f"{processed.get('diag_ym', 'NA')}_"
            f"{processed.get('sido_cd', 'NA')}_"
            f"{processed.get('sggu_cd', 'NA')}_"
            f"{processed.get('meft_div_no', 'NA')}_drug_usage"
        )
        return processed

    def _safe_int(self, value: Any) -> Optional[int]:
        if value is None or value == "":
            return None
        try:
            return int(str(value).strip().replace(",", ""))
        except (ValueError, TypeError):
            return None

    def _safe_float(self, value: Any) -> Optional[float]:
        if value is None or value == "":
            return None
        try:
            return float(str(value).strip().replace(",", ""))
        except (ValueError, TypeError):
            return None

    def collect_and_save(
        self,
        collect_medical_cost: bool = True,
        collect_medical_type: bool = True,
        collect_drug_usage: bool = True,
    ) -> Dict[str, int]:
        """
        데이터 수집 후 Elasticsearch에 저장

        Returns:
            저장 결과 통계
        """
        data = self.collect(
            collect_medical_cost=collect_medical_cost,
            collect_medical_type=collect_medical_type,
            collect_drug_usage=collect_drug_usage,
        )

        if not data:
            return {"success": 0, "failed": 0}

        return self.save_to_es(
            data=data,
            index_name=self.es_index,
            id_field="_doc_id"
        )

    def run(self) -> Dict[str, int]:
        """
        수집기 실행 (수집 -> ES 저장)

        Returns:
            저장 결과 통계
        """
        return self.collect_and_save()


if __name__ == "__main__":
    collector = HealthStatCollector()

    data = collector.collect(
        collect_medical_cost=True,
        collect_medical_type=True,
        collect_drug_usage=False,
    )

    print(f"수집된 데이터 수: {len(data)}")
    if data:
        print("첫 번째 데이터 샘플:")
        for k, v in data[0].items():
            print(f"  {k}: {v}")
