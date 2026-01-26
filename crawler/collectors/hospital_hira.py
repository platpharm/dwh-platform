"""
건강보험심사평가원 병원 정보 수집기 (Hospital HIRA Collector)

건강보험심사평가원의 병원 기본정보 조회 서비스 API를 사용하여
전국 병원/의원 정보를 수집합니다.

API 정보:
    - Base URL: http://apis.data.go.kr/B551182/hospInfoServicev2
    - Endpoint: /getHospBasisList
    - 인증: data.go.kr API Key (serviceKey)

ES 인덱스: hospital_hira
"""

import sys
import time
from typing import Any, Dict, List, Optional
from xml.etree import ElementTree

# 상위 디렉토리의 모듈 임포트를 위한 경로 설정
sys.path.append("..")

from base_collector import BaseCollector
from config import CONFIG


class HospitalHIRACollector(BaseCollector):
    """
    건강보험심사평가원 병원 정보 수집기

    전국 시도별로 순회하며 모든 병원/의원 정보를 수집합니다.
    종별코드(clCd)를 통해 병원 유형별로 필터링할 수 있습니다.

    Attributes:
        BASE_URL (str): API Base URL
        ENDPOINT (str): API Endpoint
        ES_INDEX (str): Elasticsearch 인덱스명
        PAGE_SIZE (int): 한 페이지당 조회 건수
    """

    BASE_URL = "http://apis.data.go.kr/B551182/hospInfoServicev2"
    ENDPOINT = "/getHospBasisList"
    ES_INDEX = "hospital_hira"
    PAGE_SIZE = 100

    # 시도 코드 (건강보험심사평가원 기준)
    SIDO_CODES = {
        "110000": "서울",
        "210000": "부산",
        "220000": "인천",
        "230000": "대구",
        "240000": "광주",
        "250000": "대전",
        "260000": "울산",
        "310000": "경기",
        "320000": "강원",
        "330000": "충북",
        "340000": "충남",
        "350000": "전북",
        "360000": "전남",
        "370000": "경북",
        "380000": "경남",
        "390000": "제주",
        "410000": "세종",
    }

    # 종별코드 (clCd) - 병원 유형 분류
    HOSPITAL_TYPE_CODES = {
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
        """HospitalHIRACollector 초기화"""
        super().__init__(name="HospitalHIRACollector")
        self.service_key = CONFIG["data_go_kr"]["service_key"]

        if not self.service_key:
            self.logger.warning(
                "DATA_GO_KR_API_KEY가 설정되지 않았습니다. "
                ".env 파일을 확인하세요."
            )

    def collect(
        self,
        cl_cd: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        전국 병원 정보 수집

        전국 시도별로 순회하며 모든 병원 정보를 수집합니다.
        페이지네이션을 통해 시도별 전체 데이터를 수집합니다.

        Args:
            cl_cd: 종별코드 (선택). None이면 모든 종류의 병원 수집

        Returns:
            수집된 병원 정보 리스트
        """
        all_hospitals = []

        self.logger.info("건강보험심사평가원 병원 정보 수집 시작")
        if cl_cd:
            cl_nm = self.HOSPITAL_TYPE_CODES.get(cl_cd, "알 수 없음")
            self.logger.info(f"종별코드 필터: {cl_cd} ({cl_nm})")

        for sido_cd, sido_nm in self.SIDO_CODES.items():
            self.logger.info(f"[{sido_nm}] 병원 수집 시작 (sidoCd={sido_cd})")

            hospitals = self._collect_by_sido(sido_cd, sido_nm, cl_cd)
            all_hospitals.extend(hospitals)

            self.logger.info(f"[{sido_nm}] 병원 수집 완료: {len(hospitals)}건")

            # API 호출 부하 방지를 위한 딜레이
            time.sleep(0.1)

        self.logger.info(f"전체 병원 수집 완료: 총 {len(all_hospitals)}건")

        return all_hospitals

    def collect_by_type(
        self,
        cl_codes: List[str]
    ) -> List[Dict[str, Any]]:
        """
        특정 종별코드의 병원만 수집

        Args:
            cl_codes: 수집할 종별코드 리스트 (예: ["01", "11", "21"])

        Returns:
            수집된 병원 정보 리스트
        """
        all_hospitals = []

        for cl_cd in cl_codes:
            cl_nm = self.HOSPITAL_TYPE_CODES.get(cl_cd, "알 수 없음")
            self.logger.info(f"종별코드 {cl_cd} ({cl_nm}) 수집 시작")

            hospitals = self.collect(cl_cd=cl_cd)
            all_hospitals.extend(hospitals)

        return all_hospitals

    def collect_major_hospitals(self) -> List[Dict[str, Any]]:
        """
        주요 병원(상급종합, 종합병원, 병원)만 수집

        Returns:
            수집된 주요 병원 정보 리스트
        """
        major_codes = ["01", "11", "21"]  # 상급종합, 종합병원, 병원
        return self.collect_by_type(major_codes)

    def collect_clinics(self) -> List[Dict[str, Any]]:
        """
        의원급(의원, 치과의원, 한의원)만 수집

        Returns:
            수집된 의원급 병원 정보 리스트
        """
        clinic_codes = ["31", "51", "71"]  # 의원, 치과의원, 한의원
        return self.collect_by_type(clinic_codes)

    def _collect_by_sido(
        self,
        sido_cd: str,
        sido_nm: str,
        cl_cd: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        특정 시도의 모든 병원 정보 수집

        Args:
            sido_cd: 시도 코드
            sido_nm: 시도 이름 (로깅용)
            cl_cd: 종별코드 (선택)

        Returns:
            해당 시도의 병원 정보 리스트
        """
        hospitals = []
        page_no = 1
        total_count = None

        while True:
            # API 호출
            result = self._request_hospital_list(sido_cd, page_no, cl_cd)

            if result is None:
                self.logger.error(
                    f"[{sido_nm}] 페이지 {page_no} 조회 실패, 다음 시도로 이동"
                )
                break

            # 전체 건수 확인 (첫 페이지에서만)
            if total_count is None:
                total_count = result.get("total_count", 0)
                self.logger.info(f"[{sido_nm}] 전체 병원 수: {total_count}건")

            # 데이터 추출
            items = result.get("items", [])
            if not items:
                break

            hospitals.extend(items)

            # 다음 페이지 확인
            if len(hospitals) >= total_count:
                break

            page_no += 1

            # API 호출 부하 방지를 위한 딜레이
            time.sleep(0.05)

        return hospitals

    def _request_hospital_list(
        self,
        sido_cd: str,
        page_no: int,
        cl_cd: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """
        병원 목록 API 호출

        Args:
            sido_cd: 시도 코드
            page_no: 페이지 번호
            cl_cd: 종별코드 (선택)

        Returns:
            API 응답 데이터 (items, total_count) 또는 None
        """
        url = f"{self.BASE_URL}{self.ENDPOINT}"

        params = {
            "serviceKey": self.service_key,
            "pageNo": page_no,
            "numOfRows": self.PAGE_SIZE,
            "sidoCd": sido_cd,
        }

        # 종별코드 필터 추가
        if cl_cd:
            params["clCd"] = cl_cd

        response = self._make_request(url, params=params, max_retries=3)

        if response is None:
            return None

        # XML 응답 파싱
        return self._parse_api_response(response.text)

    def _parse_api_response(
        self,
        xml_text: str
    ) -> Optional[Dict[str, Any]]:
        """
        API XML 응답 파싱

        Args:
            xml_text: XML 응답 텍스트

        Returns:
            파싱된 데이터 (items, total_count) 또는 None
        """
        try:
            root = ElementTree.fromstring(xml_text)

            # 에러 체크
            result_code = root.findtext(".//resultCode")
            if result_code != "00":
                result_msg = root.findtext(".//resultMsg")
                self.logger.error(f"API 에러: {result_code} - {result_msg}")
                return None

            # 전체 건수
            total_count = int(root.findtext(".//totalCount") or 0)

            # 아이템 파싱
            items = []
            for item in root.findall(".//item"):
                hospital = self._parse_item(item)
                if hospital:
                    items.append(hospital)

            return {
                "items": items,
                "total_count": total_count,
            }

        except ElementTree.ParseError as e:
            self.logger.error(f"XML 파싱 오류: {e}")
            return None

    def _parse_item(self, item: ElementTree.Element) -> Optional[Dict[str, Any]]:
        """
        개별 병원 정보 파싱

        Args:
            item: XML item 엘리먼트

        Returns:
            파싱된 병원 정보 딕셔너리
        """
        try:
            hospital = {
                # 기본 정보
                "ykiho": item.findtext("ykiho"),  # 요양기관 기호 (PK)
                "yadmNm": item.findtext("yadmNm"),  # 병원명
                "clCd": item.findtext("clCd"),  # 종별코드
                "clCdNm": item.findtext("clCdNm"),  # 종별코드명
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
                "hospUrl": item.findtext("hospUrl"),  # 홈페이지 URL
                # 좌표 (WGS84)
                "XPos": self._parse_float(item.findtext("XPos")),  # 경도
                "YPos": self._parse_float(item.findtext("YPos")),  # 위도
                # 개설 정보
                "estbDd": item.findtext("estbDd"),  # 개설일자
                # 의사 수
                "drTotCnt": self._parse_int(item.findtext("drTotCnt")),  # 총 의사 수
                "gdrCnt": self._parse_int(item.findtext("gdrCnt")),  # 일반의 수
                "sdrCnt": self._parse_int(item.findtext("sdrCnt")),  # 전문의 수
                "intnCnt": self._parse_int(item.findtext("intnCnt")),  # 인턴 수
                "rsdntCnt": self._parse_int(item.findtext("rsdntCnt")),  # 레지던트 수
            }

            # None 값 필터링 (선택적)
            hospital = {k: v for k, v in hospital.items() if v is not None}

            return hospital

        except Exception as e:
            self.logger.error(f"병원 정보 파싱 오류: {e}")
            return None

    def _parse_float(self, value: Optional[str]) -> Optional[float]:
        """
        문자열을 float로 변환

        Args:
            value: 변환할 문자열

        Returns:
            변환된 float 또는 None
        """
        if value is None or value.strip() == "":
            return None
        try:
            return float(value)
        except ValueError:
            return None

    def _parse_int(self, value: Optional[str]) -> Optional[int]:
        """
        문자열을 int로 변환

        Args:
            value: 변환할 문자열

        Returns:
            변환된 int 또는 None
        """
        if value is None or value.strip() == "":
            return None
        try:
            return int(value)
        except ValueError:
            return None

    def run(self, cl_cd: Optional[str] = None) -> Dict[str, int]:
        """
        수집 및 Elasticsearch 저장 실행

        Args:
            cl_cd: 종별코드 (선택). None이면 모든 병원 수집

        Returns:
            저장 결과 통계 (success, failed 카운트)
        """
        self.logger.info("HospitalHIRACollector 실행 시작")

        # 데이터 수집
        hospitals = self.collect(cl_cd=cl_cd)

        if not hospitals:
            self.logger.warning("수집된 병원 정보가 없습니다.")
            return {"success": 0, "failed": 0}

        # Elasticsearch 저장
        result = self.save_to_es(
            data=hospitals,
            index_name=self.ES_INDEX,
            id_field="ykiho"  # 요양기관 기호를 문서 ID로 사용
        )

        self.logger.info(
            f"HospitalHIRACollector 실행 완료: "
            f"성공={result['success']}, 실패={result['failed']}"
        )

        return result


# CLI 실행 지원
if __name__ == "__main__":
    collector = HospitalHIRACollector()
    collector.run()
