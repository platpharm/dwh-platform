"""
의료기관 운영상태 수집기 (Business Status Collector)

건강보험심사평가원 API를 사용하여 약국/의료기관의 운영 상태를 수집합니다.
폐업/휴업 여부를 파악하여 기존 pharmacy_hira, hospital_hira 데이터 품질을 보완합니다.

API 정보:
    - 약국: http://apis.data.go.kr/B551182/pharmacyInfoService/getParmacyBasisList
    - 병원: http://apis.data.go.kr/B551182/hospInfoServicev2/getHospBasisList
    - 인증: data.go.kr API Key (serviceKey)

ES 인덱스: business_status
"""

import time
from datetime import datetime
from typing import Any, Dict, List, Optional
from xml.etree import ElementTree

from crawler.base_collector import BaseCollector
from crawler.config import CONFIG


PHARMACY_API = {
    "base_url": "http://apis.data.go.kr/B551182/pharmacyInfoService",
    "endpoint": "/getParmacyBasisList",
}

HOSPITAL_API = {
    "base_url": "http://apis.data.go.kr/B551182/hospInfoServicev2",
    "endpoint": "/getHospBasisList",
}

HOSPITAL_CL_CODES = {
    "01": "상급종합병원",
    "11": "종합병원",
    "21": "병원",
    "28": "요양병원",
    "29": "정신병원",
    "31": "의원",
    "41": "치과병원",
    "51": "치과의원",
    "61": "조산원",
    "71": "보건소",
    "72": "보건지소",
    "73": "보건진료소",
    "75": "보건의료원",
    "91": "한방병원",
    "92": "한의원",
}


class BusinessStatusCollector(BaseCollector):
    """
    의료기관 운영상태 수집기

    약국은 pharmacyInfoService, 병원은 hospInfoServicev2를 사용하여
    전국 시도별로 운영 상태를 수집합니다.
    """

    ES_INDEX = "business_status"
    PAGE_SIZE = 100

    SIDO_CODES = {
        "110000": "서울", "210000": "부산", "220000": "인천",
        "230000": "대구", "240000": "광주", "250000": "대전",
        "260000": "울산", "310000": "경기", "320000": "강원",
        "330000": "충북", "340000": "충남", "350000": "전북",
        "360000": "전남", "370000": "경북", "380000": "경남",
        "390000": "제주", "410000": "세종",
    }

    TARGET_HOSPITAL_CL_CODES = ["31", "01", "11", "21"]

    def __init__(self):
        super().__init__(name="BusinessStatusCollector")
        self.service_key = CONFIG["data_go_kr"]["service_key"]

        if not self.service_key:
            self.logger.warning(
                "DATA_GO_KR_API_KEY가 설정되지 않았습니다. "
                ".env 파일을 확인하세요."
            )

    def collect(self) -> List[Dict[str, Any]]:
        """
        약국 + 병원 운영상태 수집

        Returns:
            수집된 의료기관 운영상태 리스트
        """
        all_data = []

        self.logger.info("의료기관 운영상태 수집 시작")

        pharmacy_data = self._collect_pharmacies()
        all_data.extend(pharmacy_data)
        self.logger.info(f"약국 운영상태 수집 완료: {len(pharmacy_data)}건")

        hospital_data = self._collect_hospitals()
        all_data.extend(hospital_data)
        self.logger.info(f"병원 운영상태 수집 완료: {len(hospital_data)}건")

        self.logger.info(f"전체 운영상태 수집 완료: 총 {len(all_data)}건")
        return all_data

    def _collect_pharmacies(self) -> List[Dict[str, Any]]:
        all_items = []
        url = f"{PHARMACY_API['base_url']}{PHARMACY_API['endpoint']}"

        self.logger.info("[약국] 수집 시작 (pharmacyInfoService)")

        for sido_cd, sido_nm in self.SIDO_CODES.items():
            items = self._paginate_api(url, sido_cd, sido_nm, cl_cd=None)

            for item in items:
                item["clCd"] = "81"
                item["clCdNm"] = "약국"

            all_items.extend(items)
            self.logger.info(f"[약국-{sido_nm}] {len(items)}건")
            time.sleep(0.1)

        return all_items

    def _collect_hospitals(self) -> List[Dict[str, Any]]:
        all_items = []
        url = f"{HOSPITAL_API['base_url']}{HOSPITAL_API['endpoint']}"

        for cl_cd in self.TARGET_HOSPITAL_CL_CODES:
            cl_nm = HOSPITAL_CL_CODES.get(cl_cd, cl_cd)
            self.logger.info(f"[{cl_nm}] 수집 시작 (hospInfoServicev2)")

            for sido_cd, sido_nm in self.SIDO_CODES.items():
                items = self._paginate_api(url, sido_cd, sido_nm, cl_cd=cl_cd)
                all_items.extend(items)
                time.sleep(0.1)

            self.logger.info(f"[{cl_nm}] 수집 완료")

        return all_items

    def _paginate_api(
        self, url: str, sido_cd: str, sido_nm: str, cl_cd: Optional[str]
    ) -> List[Dict[str, Any]]:
        items = []
        page_no = 1
        total_count = None

        while True:
            params = {
                "serviceKey": self.service_key,
                "pageNo": page_no,
                "numOfRows": self.PAGE_SIZE,
                "sidoCd": sido_cd,
            }
            if cl_cd:
                params["clCd"] = cl_cd

            response = self._make_request(url, params=params)
            if response is None:
                break

            result = self._parse_api_response(response.text)
            if result is None:
                break

            if total_count is None:
                total_count = result.get("total_count", 0)

            page_items = result.get("items", [])
            if not page_items:
                break

            items.extend(page_items)

            if len(items) >= total_count:
                break

            page_no += 1
            time.sleep(0.05)

        return items

    def _parse_api_response(self, xml_text: str) -> Optional[Dict[str, Any]]:
        try:
            root = ElementTree.fromstring(xml_text)

            result_code = root.findtext(".//resultCode")
            if result_code != "00":
                result_msg = root.findtext(".//resultMsg")
                self.logger.error(f"API 에러: {result_code} - {result_msg}")
                return None

            total_count = int(root.findtext(".//totalCount") or 0)

            items = []
            for item in root.findall(".//item"):
                parsed = self._parse_item(item)
                if parsed:
                    items.append(parsed)

            return {"items": items, "total_count": total_count}

        except ElementTree.ParseError as e:
            self.logger.error(f"XML 파싱 오류: {e}")
            return None

    def _parse_item(self, item: ElementTree.Element) -> Optional[Dict[str, Any]]:
        try:
            collected_at = datetime.now().isoformat()
            ykiho = item.findtext("ykiho")
            cl_cd = item.findtext("clCd")

            parsed = {
                "ykiho": ykiho,
                "yadmNm": item.findtext("yadmNm"),
                "clCd": cl_cd,
                "clCdNm": (
                    item.findtext("clCdNm")
                    or HOSPITAL_CL_CODES.get(cl_cd, "")
                ),
                "sidoCd": item.findtext("sidoCd"),
                "sidoCdNm": item.findtext("sidoCdNm"),
                "sgguCd": item.findtext("sgguCd"),
                "sgguCdNm": item.findtext("sgguCdNm"),
                "emdongNm": item.findtext("emdongNm"),
                "addr": item.findtext("addr"),
                "telno": item.findtext("telno"),
                "estbDd": item.findtext("estbDd"),
                "XPos": self._parse_float(item.findtext("XPos")),
                "YPos": self._parse_float(item.findtext("YPos")),
                "drTotCnt": self._safe_int(item.findtext("drTotCnt")),
                "collected_at": collected_at,
            }

            return {k: v for k, v in parsed.items() if v is not None}

        except Exception as e:
            self.logger.error(f"의료기관 항목 파싱 오류: {e}")
            return None

    def _parse_float(self, value: Optional[str]) -> Optional[float]:
        if value is None or value.strip() == "":
            return None
        try:
            return float(value)
        except ValueError:
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
        self.logger.info("BusinessStatusCollector 실행 시작")

        data = self.collect()

        if not data:
            self.logger.warning("수집된 운영상태 데이터가 없습니다.")
            return {"success": 0, "failed": 0}

        result = self.save_to_es(
            data=data,
            index_name=self.ES_INDEX,
            id_field="ykiho"
        )

        self.logger.info(
            f"BusinessStatusCollector 실행 완료: "
            f"성공={result['success']}, 실패={result['failed']}"
        )

        return result


if __name__ == "__main__":
    collector = BusinessStatusCollector()
    collector.run()
