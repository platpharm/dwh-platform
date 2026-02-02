"""
건강보험심사평가원 진료과목 정보 수집기

데이터 소스: 건강보험심사평가원 의료기관 기본정보
API: http://apis.data.go.kr/B551182/hospInfoServicev2
엔드포인트: /getHospBasisList (dgsbjtCd 파라미터로 진료과목별 필터)

진료과목코드(dgsbjtCd)별로 시도를 순회하며 해당 과목을 보유한
의료기관 목록과 의사 수를 수집합니다.

ES 인덱스: medical_department
"""

import time
from datetime import datetime
from typing import Any, Dict, List, Optional
from xml.etree import ElementTree

from crawler.base_collector import BaseCollector
from crawler.config import CONFIG


DEPARTMENT_CODES = {
    "00": "일반의",
    "01": "내과",
    "02": "신경과",
    "03": "정신건강의학과",
    "04": "외과",
    "05": "정형외과",
    "06": "신경외과",
    "07": "흉부외과",
    "08": "성형외과",
    "09": "마취통증의학과",
    "10": "산부인과",
    "11": "소아청소년과",
    "12": "안과",
    "13": "이비인후과",
    "14": "피부과",
    "15": "비뇨의학과",
    "16": "영상의학과",
    "17": "방사선종양학과",
    "18": "병리과",
    "19": "진단검사의학과",
    "20": "결핵과",
    "21": "재활의학과",
    "22": "핵의학과",
    "23": "가정의학과",
    "24": "응급의학과",
    "25": "직업환경의학과",
    "26": "예방의학과",
    "49": "치과",
    "80": "한방내과",
    "81": "한방부인과",
    "82": "한방소아과",
    "83": "한방안이비인후피부과",
    "84": "한방신경정신과",
    "85": "침구과",
    "86": "한방재활의학과",
    "87": "사상체질과",
    "88": "한방응급",
}

PRIMARY_DEPARTMENT_CODES = [
    "00", "01", "02", "03", "04", "05", "06", "07", "08", "09",
    "10", "11", "12", "13", "14", "15", "21", "23", "24", "49",
]


class MedicalDepartmentCollector(BaseCollector):
    """
    건강보험심사평가원 진료과목 정보 수집기

    getHospBasisList API에 dgsbjtCd 파라미터를 사용하여
    진료과목별 의료기관 목록과 의사 수를 수집합니다.
    """

    BASE_URL = "http://apis.data.go.kr/B551182/hospInfoServicev2"
    ENDPOINT = "/getHospBasisList"
    ES_INDEX = "medical_department"
    PAGE_SIZE = 100

    SIDO_CODES = {
        "110000": "서울", "210000": "부산", "220000": "인천",
        "230000": "대구", "240000": "광주", "250000": "대전",
        "260000": "울산", "310000": "경기", "320000": "강원",
        "330000": "충북", "340000": "충남", "350000": "전북",
        "360000": "전남", "370000": "경북", "380000": "경남",
        "390000": "제주", "410000": "세종",
    }

    def __init__(self):
        super().__init__(name="MedicalDepartmentCollector")
        self.service_key = CONFIG["data_go_kr"]["service_key"]

        if not self.service_key:
            self.logger.warning(
                "DATA_GO_KR_API_KEY가 설정되지 않았습니다. "
                ".env 파일을 확인하세요."
            )

    def collect(
        self, dgsbj_codes: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        """
        진료과목별 의료기관 정보 수집

        Args:
            dgsbj_codes: 수집할 진료과목코드 목록. None이면 주요 20개 과목

        Returns:
            수집된 진료과목별 의료기관 리스트
        """
        dgsbj_codes = dgsbj_codes or PRIMARY_DEPARTMENT_CODES
        all_data = []

        self.logger.info(
            f"진료과목 정보 수집 시작 ({len(dgsbj_codes)}개 과목)"
        )

        for dgsbj_cd in dgsbj_codes:
            dgsbj_nm = DEPARTMENT_CODES.get(dgsbj_cd, dgsbj_cd)
            self.logger.info(f"[{dgsbj_nm}] 수집 시작")

            dept_items = []
            for sido_cd, sido_nm in self.SIDO_CODES.items():
                items = self._collect_by_sido_and_dept(
                    sido_cd, sido_nm, dgsbj_cd, dgsbj_nm
                )
                dept_items.extend(items)
                time.sleep(0.1)

            all_data.extend(dept_items)
            self.logger.info(f"[{dgsbj_nm}] 수집 완료: {len(dept_items)}건")

        self.logger.info(f"전체 진료과목 수집 완료: 총 {len(all_data)}건")
        return all_data

    def _collect_by_sido_and_dept(
        self,
        sido_cd: str,
        sido_nm: str,
        dgsbj_cd: str,
        dgsbj_nm: str,
    ) -> List[Dict[str, Any]]:
        items = []
        page_no = 1
        total_count = None

        while True:
            result = self._request_list(sido_cd, dgsbj_cd, page_no)

            if result is None:
                break

            if total_count is None:
                total_count = result.get("total_count", 0)

            page_items = result.get("items", [])
            if not page_items:
                break

            for raw_item in page_items:
                raw_item["dgsbjtCd"] = dgsbj_cd
                raw_item["dgsbjtCdNm"] = dgsbj_nm

            items.extend(page_items)

            if len(items) >= total_count:
                break

            page_no += 1
            time.sleep(0.05)

        return items

    def _request_list(
        self, sido_cd: str, dgsbj_cd: str, page_no: int
    ) -> Optional[Dict[str, Any]]:
        url = f"{self.BASE_URL}{self.ENDPOINT}"

        params = {
            "serviceKey": self.service_key,
            "pageNo": page_no,
            "numOfRows": self.PAGE_SIZE,
            "sidoCd": sido_cd,
            "dgsbjtCd": dgsbj_cd,
        }

        response = self._make_request(url, params=params)

        if response is None:
            return None

        return self._parse_api_response(response.text)

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
            ykiho = item.findtext("ykiho") or ""

            parsed = {
                "ykiho": ykiho,
                "yadmNm": item.findtext("yadmNm"),
                "clCd": item.findtext("clCd"),
                "clCdNm": item.findtext("clCdNm"),
                "sidoCd": item.findtext("sidoCd"),
                "sidoCdNm": item.findtext("sidoCdNm"),
                "sgguCd": item.findtext("sgguCd"),
                "sgguCdNm": item.findtext("sgguCdNm"),
                "addr": item.findtext("addr"),
                "drTotCnt": self._safe_int(item.findtext("drTotCnt")),
                "mdeptSdrCnt": self._safe_int(item.findtext("mdeptSdrCnt")),
                "mdeptGdrCnt": self._safe_int(item.findtext("mdeptGdrCnt")),
                "mdeptIntnCnt": self._safe_int(item.findtext("mdeptIntnCnt")),
                "mdeptResdntCnt": self._safe_int(item.findtext("mdeptResdntCnt")),
                "XPos": self._parse_float(item.findtext("XPos")),
                "YPos": self._parse_float(item.findtext("YPos")),
                "collected_at": collected_at,
            }

            return {k: v for k, v in parsed.items() if v is not None}

        except Exception as e:
            self.logger.error(f"진료과목 항목 파싱 오류: {e}")
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
        self.logger.info("MedicalDepartmentCollector 실행 시작")

        data = self.collect()

        if not data:
            self.logger.warning("수집된 진료과목 데이터가 없습니다.")
            return {"success": 0, "failed": 0}

        result = self.save_to_es(
            data=data,
            index_name=self.ES_INDEX,
            id_field="_doc_id"
        )

        self.logger.info(
            f"MedicalDepartmentCollector 실행 완료: "
            f"성공={result['success']}, 실패={result['failed']}"
        )

        return result


if __name__ == "__main__":
    collector = MedicalDepartmentCollector()
    collector.run()
