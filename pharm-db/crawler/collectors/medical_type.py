"""
Medical Type Collector Module

건강보험심사평가원 의료기관 종별 정보를 수집합니다.
종별코드(clCd)별로 의료기관 목록을 API로 수집하여 Elasticsearch에 저장합니다.

API 정보:
    - Base URL: http://apis.data.go.kr/B551182/hospInfoServicev2
    - Endpoint: /getHospBasisList
    - 인증: data.go.kr API Key (serviceKey)

ES 인덱스: medical_type
"""

import time
from datetime import datetime
from typing import Any, Dict, List, Optional
from xml.etree import ElementTree

from crawler.base_collector import BaseCollector
from crawler.config import CONFIG


class MedicalTypeCollector(BaseCollector):
    """
    의료기관 종별 수집기

    건강보험심사평가원 의료기관정보서비스 API를 사용하여
    종별코드별 의료기관 목록을 수집합니다.

    Attributes:
        BASE_URL (str): API Base URL
        ENDPOINT (str): API Endpoint
        ES_INDEX (str): Elasticsearch 인덱스명
        PAGE_SIZE (int): 한 페이지당 조회 건수
    """

    BASE_URL = "http://apis.data.go.kr/B551182/hospInfoServicev2"
    ENDPOINT = "/getHospBasisList"
    ES_INDEX = "medical_type"
    PAGE_SIZE = 100

    # 종별코드 정의
    MEDICAL_TYPE_CODES = {
        "01": "상급종합병원",
        "11": "종합병원",
        "21": "병원",
        "28": "요양병원",
        "29": "정신병원",
        "31": "의원",
        "41": "치과병원",
        "51": "치과의원",
        "61": "한방병원",
        "71": "한의원",
        "81": "조산원",
        "91": "보건소",
        "92": "보건지소",
        "93": "보건진료소",
    }

    def __init__(self):
        """MedicalTypeCollector 초기화"""
        super().__init__(name="MedicalTypeCollector")
        self.service_key = CONFIG["data_go_kr"]["service_key"]

        if not self.service_key:
            self.logger.warning(
                "DATA_GO_KR_API_KEY가 설정되지 않았습니다. "
                ".env 파일을 확인하세요."
            )

    def collect(
        self,
        cl_codes: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        """
        의료기관 종별 정보 수집

        종별코드별로 순회하며 모든 의료기관 정보를 수집합니다.
        페이지네이션을 통해 종별코드별 전체 데이터를 수집합니다.

        Args:
            cl_codes: 수집할 종별코드 리스트 (None이면 전체)

        Returns:
            수집된 의료기관 정보 리스트
        """
        all_institutions = []

        self.logger.info("건강보험심사평가원 의료기관 종별 정보 수집 시작")

        codes_to_collect = cl_codes or list(self.MEDICAL_TYPE_CODES.keys())

        for cl_cd in codes_to_collect:
            if cl_cd not in self.MEDICAL_TYPE_CODES:
                self.logger.warning(f"알 수 없는 종별코드: {cl_cd}")
                continue

            cl_name = self.MEDICAL_TYPE_CODES[cl_cd]
            self.logger.info(f"[{cl_name}] 의료기관 수집 시작 (clCd={cl_cd})")

            institutions = self._collect_by_class_code(cl_cd, cl_name)
            all_institutions.extend(institutions)

            self.logger.info(
                f"[{cl_name}] 의료기관 수집 완료: {len(institutions)}건"
            )

            # API 호출 부하 방지를 위한 딜레이
            time.sleep(0.1)

        self.logger.info(
            f"전체 의료기관 종별 수집 완료: 총 {len(all_institutions)}건"
        )

        return all_institutions

    def _collect_by_class_code(
        self,
        cl_cd: str,
        cl_name: str
    ) -> List[Dict[str, Any]]:
        """
        특정 종별코드의 모든 의료기관 정보 수집

        Args:
            cl_cd: 종별코드
            cl_name: 종별코드명 (로깅용)

        Returns:
            해당 종별코드의 의료기관 정보 리스트
        """
        institutions = []
        page_no = 1
        total_count = None

        while True:
            result = self._request_institution_list(cl_cd, page_no)

            if result is None:
                self.logger.error(
                    f"[{cl_name}] 페이지 {page_no} 조회 실패, 다음 종별코드로 이동"
                )
                break

            if total_count is None:
                total_count = result.get("total_count", 0)
                self.logger.info(f"[{cl_name}] 전체 의료기관 수: {total_count}건")

            items = result.get("items", [])
            if not items:
                break

            institutions.extend(items)

            if len(institutions) >= total_count:
                break

            page_no += 1

            # API 호출 부하 방지를 위한 딜레이
            time.sleep(0.05)

        return institutions

    def _request_institution_list(
        self,
        cl_cd: str,
        page_no: int
    ) -> Optional[Dict[str, Any]]:
        """
        의료기관 목록 API 호출

        Args:
            cl_cd: 종별코드
            page_no: 페이지 번호

        Returns:
            API 응답 데이터 (items, total_count) 또는 None
        """
        url = f"{self.BASE_URL}{self.ENDPOINT}"

        params = {
            "serviceKey": self.service_key,
            "pageNo": page_no,
            "numOfRows": self.PAGE_SIZE,
            "clCd": cl_cd,
            "_type": "json",  # JSON 응답 요청
        }

        response = self._make_request(url, params=params)

        if response is None:
            return None

        return self._parse_api_response(response, cl_cd)

    def _parse_api_response(
        self,
        response,
        cl_cd: str
    ) -> Optional[Dict[str, Any]]:
        """
        API JSON 응답 파싱

        Args:
            response: HTTP 응답 객체
            cl_cd: 종별코드 (파싱 시 기본값으로 사용)

        Returns:
            파싱된 데이터 (items, total_count) 또는 None
        """
        try:
            data = self._parse_json_response(response)

            if data is None:
                return self._parse_xml_response(response.text, cl_cd)

            result_code = (
                data.get("response", {})
                .get("header", {})
                .get("resultCode", "")
            )

            if result_code != "00":
                result_msg = (
                    data.get("response", {})
                    .get("header", {})
                    .get("resultMsg", "Unknown error")
                )
                self.logger.error(f"API 에러: {result_code} - {result_msg}")
                return None

            body = data.get("response", {}).get("body", {})
            total_count = int(body.get("totalCount", 0))

            items_data = body.get("items", {})

            if items_data == "" or items_data is None:
                return {"items": [], "total_count": 0}

            item_list = items_data.get("item", [])

            if isinstance(item_list, dict):
                item_list = [item_list]

            items = []
            for item in item_list:
                parsed = self._parse_item(item, cl_cd)
                if parsed:
                    items.append(parsed)

            return {
                "items": items,
                "total_count": total_count,
            }

        except Exception as e:
            self.logger.error(f"응답 파싱 오류: {e}")
            return None

    def _parse_xml_response(
        self,
        xml_text: str,
        cl_cd: str
    ) -> Optional[Dict[str, Any]]:
        """
        API XML 응답 파싱 (JSON 파싱 실패 시 대체)

        Args:
            xml_text: XML 응답 텍스트
            cl_cd: 종별코드

        Returns:
            파싱된 데이터 (items, total_count) 또는 None
        """
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
                parsed = self._parse_xml_item(item, cl_cd)
                if parsed:
                    items.append(parsed)

            return {
                "items": items,
                "total_count": total_count,
            }

        except ElementTree.ParseError as e:
            self.logger.error(f"XML 파싱 오류: {e}")
            return None

    def _parse_item(
        self,
        item: Dict[str, Any],
        cl_cd: str
    ) -> Optional[Dict[str, Any]]:
        """
        개별 의료기관 정보 파싱 (JSON)

        Args:
            item: JSON 아이템 딕셔너리
            cl_cd: 종별코드 (기본값)

        Returns:
            파싱된 의료기관 정보 딕셔너리
        """
        try:
            institution = {
                # 기본 정보
                "ykiho": item.get("ykiho"),  # 암호화된 요양기호 (PK)
                "yadmNm": item.get("yadmNm"),  # 요양기관명
                "clCd": item.get("clCd") or cl_cd,  # 종별코드
                "clCdNm": (
                    item.get("clCdNm") or
                    self.MEDICAL_TYPE_CODES.get(cl_cd, "")
                ),  # 종별코드명
                # 위치 정보
                "sidoCd": item.get("sidoCd"),  # 시도코드
                "sidoCdNm": item.get("sidoCdNm"),  # 시도명
                "sgguCd": item.get("sgguCd"),  # 시군구코드
                "sgguCdNm": item.get("sgguCdNm"),  # 시군구명
                "emdongNm": item.get("emdongNm"),  # 읍면동명
                "addr": item.get("addr"),  # 주소
                "postNo": item.get("postNo"),  # 우편번호
                # 연락처
                "telno": item.get("telno"),  # 전화번호
                "hospUrl": item.get("hospUrl"),  # 홈페이지
                # 좌표 (WGS84)
                "XPos": self._parse_float(item.get("XPos")),  # 경도
                "YPos": self._parse_float(item.get("YPos")),  # 위도
                # 개설일자
                "estbDd": item.get("estbDd"),  # 개설일자
                # 인력 정보
                "drTotCnt": self._parse_int(item.get("drTotCnt")),  # 의사 총수
                "sdrCnt": self._parse_int(item.get("sdrCnt")),  # 전문의 수
                "gdrCnt": self._parse_int(item.get("gdrCnt")),  # 일반의 수
                "intnCnt": self._parse_int(item.get("intnCnt")),  # 인턴 수
                "resdntCnt": self._parse_int(item.get("resdntCnt")),  # 레지던트 수
                "pnursCnt": self._parse_int(item.get("pnursCnt")),  # 간호사 수
                # 메타 정보
                "collected_at": datetime.now().isoformat(),
            }

            institution = {k: v for k, v in institution.items() if v is not None}

            return institution

        except Exception as e:
            self.logger.error(f"의료기관 정보 파싱 오류: {e}")
            return None

    def _parse_xml_item(
        self,
        item: ElementTree.Element,
        cl_cd: str
    ) -> Optional[Dict[str, Any]]:
        """
        개별 의료기관 정보 파싱 (XML)

        Args:
            item: XML item 엘리먼트
            cl_cd: 종별코드 (기본값)

        Returns:
            파싱된 의료기관 정보 딕셔너리
        """
        try:
            institution = {
                # 기본 정보
                "ykiho": item.findtext("ykiho"),  # 암호화된 요양기호 (PK)
                "yadmNm": item.findtext("yadmNm"),  # 요양기관명
                "clCd": item.findtext("clCd") or cl_cd,  # 종별코드
                "clCdNm": (
                    item.findtext("clCdNm") or
                    self.MEDICAL_TYPE_CODES.get(cl_cd, "")
                ),  # 종별코드명
                # 위치 정보
                "sidoCd": item.findtext("sidoCd"),  # 시도코드
                "sidoCdNm": item.findtext("sidoCdNm"),  # 시도명
                "sgguCd": item.findtext("sgguCd"),  # 시군구코드
                "sgguCdNm": item.findtext("sgguCdNm"),  # 시군구명
                "emdongNm": item.findtext("emdongNm"),  # 읍면동명
                "addr": item.findtext("addr"),  # 주소
                "postNo": item.findtext("postNo"),  # 우편번호
                # 연락처
                "telno": item.findtext("telno"),  # 전화번호
                "hospUrl": item.findtext("hospUrl"),  # 홈페이지
                # 좌표 (WGS84)
                "XPos": self._parse_float(item.findtext("XPos")),  # 경도
                "YPos": self._parse_float(item.findtext("YPos")),  # 위도
                # 개설일자
                "estbDd": item.findtext("estbDd"),  # 개설일자
                # 인력 정보
                "drTotCnt": self._parse_int(item.findtext("drTotCnt")),  # 의사 총수
                "sdrCnt": self._parse_int(item.findtext("sdrCnt")),  # 전문의 수
                "gdrCnt": self._parse_int(item.findtext("gdrCnt")),  # 일반의 수
                "intnCnt": self._parse_int(item.findtext("intnCnt")),  # 인턴 수
                "resdntCnt": self._parse_int(item.findtext("resdntCnt")),  # 레지던트 수
                "pnursCnt": self._parse_int(item.findtext("pnursCnt")),  # 간호사 수
                # 메타 정보
                "collected_at": datetime.now().isoformat(),
            }

            institution = {k: v for k, v in institution.items() if v is not None}

            return institution

        except Exception as e:
            self.logger.error(f"의료기관 정보 파싱 오류: {e}")
            return None

    def _parse_float(self, value: Optional[str]) -> Optional[float]:
        """
        문자열을 float로 변환

        Args:
            value: 변환할 문자열

        Returns:
            변환된 float 또는 None
        """
        if value is None or str(value).strip() == "":
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None

    def _parse_int(self, value: Optional[str]) -> Optional[int]:
        """
        문자열을 int로 변환

        Args:
            value: 변환할 문자열

        Returns:
            변환된 int 또는 None
        """
        if value is None or str(value).strip() == "":
            return None
        try:
            return int(value)
        except (ValueError, TypeError):
            return None

    def run(
        self,
        cl_codes: Optional[List[str]] = None
    ) -> Dict[str, int]:
        """
        수집 및 Elasticsearch 저장 실행

        Args:
            cl_codes: 수집할 종별코드 리스트 (None이면 전체)

        Returns:
            저장 결과 통계 (success, failed 카운트)
        """
        self.logger.info("MedicalTypeCollector 실행 시작")

        institutions = self.collect(cl_codes=cl_codes)

        if not institutions:
            self.logger.warning("수집된 의료기관 정보가 없습니다.")
            return {"success": 0, "failed": 0}

        result = self.save_to_es(
            data=institutions,
            index_name=self.ES_INDEX,
            id_field="ykiho"  # 암호화된 요양기호를 문서 ID로 사용
        )

        self.logger.info(
            f"MedicalTypeCollector 실행 완료: "
            f"성공={result['success']}, 실패={result['failed']}"
        )

        return result


if __name__ == "__main__":
    collector = MedicalTypeCollector()

    # 테스트: 상급종합병원(01)만 수집
    print("테스트 모드: 상급종합병원(01) 종별코드 수집")
    test_data = collector.collect(cl_codes=["01"])

    print(f"\n수집된 데이터 수: {len(test_data)}")
    if test_data:
        print("\n첫 번째 데이터 샘플:")
        sample = test_data[0]
        for key, value in sample.items():
            print(f"  {key}: {value}")
