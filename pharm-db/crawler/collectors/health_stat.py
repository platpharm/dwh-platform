"""
Health Statistics Collector Module

건강보험심사평가원 보건의료 통계 데이터를 수집합니다.
- 의료비통계 API (statMdcRgnService)
- 약품처방통계 API (drugPrscStatInfoService)
"""

import time
from datetime import datetime
from typing import Any, Dict, List, Optional

from crawler.base_collector import BaseCollector
from crawler.config import CONFIG


class HealthStatCollector(BaseCollector):
    """
    건강보험심사평가원 보건의료 통계 수집기

    Attributes:
        service_key (str): 공공데이터포털 API 인증키
        base_urls (dict): API 엔드포인트 URL 목록
        es_index (str): Elasticsearch 인덱스 이름
    """

    # API 엔드포인트
    BASE_URLS = {
        "medical_cost": "http://apis.data.go.kr/B551182/statMdcRgnService",
        "drug_prsc": "http://apis.data.go.kr/B551182/drugPrscStatInfoService",
    }

    # 시도 코드 매핑 (건강보험심사평가원 기준)
    SIDO_CODES = {
        "11": "서울",
        "21": "부산",
        "22": "대구",
        "23": "인천",
        "24": "광주",
        "25": "대전",
        "26": "울산",
        "29": "세종",
        "31": "경기",
        "32": "강원",
        "33": "충북",
        "34": "충남",
        "35": "전북",
        "36": "전남",
        "37": "경북",
        "38": "경남",
        "39": "제주",
    }

    # 기본 페이징 설정
    DEFAULT_NUM_OF_ROWS = 100

    def __init__(self):
        """HealthStatCollector 초기화"""
        super().__init__(name="HealthStatCollector")
        self.service_key = CONFIG["data_go_kr"]["service_key"]
        self.es_index = "health_stat"

        if not self.service_key:
            self.logger.warning(
                "DATA_GO_KR_API_KEY 환경 변수가 설정되지 않았습니다."
            )

    def collect(
        self,
        st_yy: Optional[str] = None,
        sido_cd: Optional[str] = None,
        collect_medical_cost: bool = True,
        collect_drug_prsc: bool = True,
    ) -> List[Dict[str, Any]]:
        """
        보건의료 통계 데이터 수집

        Args:
            st_yy: 기준년도 (예: "2023"), None이면 전년도
            sido_cd: 시도코드 (예: "11"=서울), None이면 전체 시도
            collect_medical_cost: 의료비통계 수집 여부
            collect_drug_prsc: 약품처방통계 수집 여부

        Returns:
            수집된 통계 데이터 리스트
        """
        if not self.service_key:
            self.logger.error("API 키가 설정되지 않아 수집을 중단합니다.")
            return []

        # 기준년도 설정 (기본값: 전년도)
        if st_yy is None:
            st_yy = str(datetime.now().year - 1)

        all_data = []

        sido_codes_to_collect = (
            [sido_cd] if sido_cd else list(self.SIDO_CODES.keys())
        )

        if collect_medical_cost:
            self.logger.info(f"의료비통계 수집 시작 (기준년도: {st_yy})")
            medical_cost_data = self._collect_medical_cost_stats(
                st_yy, sido_codes_to_collect
            )
            all_data.extend(medical_cost_data)
            self.logger.info(f"의료비통계 수집 완료: {len(medical_cost_data)}건")

        if collect_drug_prsc:
            self.logger.info(f"약품처방통계 수집 시작 (기준년도: {st_yy})")
            drug_prsc_data = self._collect_drug_prsc_stats(
                st_yy, sido_codes_to_collect
            )
            all_data.extend(drug_prsc_data)
            self.logger.info(f"약품처방통계 수집 완료: {len(drug_prsc_data)}건")

        self.logger.info(f"전체 보건의료 통계 수집 완료: 총 {len(all_data)}건")
        return all_data

    def _collect_medical_cost_stats(
        self,
        st_yy: str,
        sido_codes: List[str],
    ) -> List[Dict[str, Any]]:
        """
        의료비통계 데이터 수집

        Args:
            st_yy: 기준년도
            sido_codes: 수집할 시도코드 리스트

        Returns:
            의료비통계 데이터 리스트
        """
        all_data = []
        base_url = f"{self.BASE_URLS['medical_cost']}/getRgnMdcStatsInfo"

        for sido_cd in sido_codes:
            self.logger.info(
                f"의료비통계 수집 중: {self.SIDO_CODES.get(sido_cd, sido_cd)}"
            )
            page_no = 1
            total_count = None

            while True:
                params = {
                    "serviceKey": self.service_key,
                    "stYy": st_yy,
                    "sidoCd": sido_cd,
                    "pageNo": page_no,
                    "numOfRows": self.DEFAULT_NUM_OF_ROWS,
                    "_type": "json",
                }

                response = self._make_request(url=base_url, params=params)

                if response is None:
                    self.logger.warning(
                        f"의료비통계 요청 실패: sido_cd={sido_cd}, page={page_no}"
                    )
                    break

                data = self._parse_json_response(response)

                if data is None:
                    self.logger.warning(
                        f"의료비통계 JSON 파싱 실패: sido_cd={sido_cd}"
                    )
                    break

                items = self._extract_items_from_response(data)

                if items is None:
                    result_code = self._get_result_code(data)
                    if result_code != "00":
                        self.logger.warning(
                            f"API 응답 에러: resultCode={result_code}"
                        )
                    break

                # 전체 건수 확인 (첫 페이지에서만)
                if total_count is None:
                    total_count = self._get_total_count(data)
                    self.logger.debug(
                        f"의료비통계 전체 건수: {total_count} (sido_cd={sido_cd})"
                    )

                for item in items:
                    processed_item = self._process_medical_cost_item(item)
                    all_data.append(processed_item)

                if len(items) < self.DEFAULT_NUM_OF_ROWS:
                    break

                page_no += 1
                time.sleep(0.1)  # API 호출 간격 조절

        return all_data

    def _collect_drug_prsc_stats(
        self,
        st_yy: str,
        sido_codes: List[str],
    ) -> List[Dict[str, Any]]:
        """
        약품처방통계 데이터 수집

        Args:
            st_yy: 기준년도
            sido_codes: 수집할 시도코드 리스트

        Returns:
            약품처방통계 데이터 리스트
        """
        all_data = []
        base_url = f"{self.BASE_URLS['drug_prsc']}/getDrugPrscRgnStatInfo"

        for sido_cd in sido_codes:
            self.logger.info(
                f"약품처방통계 수집 중: {self.SIDO_CODES.get(sido_cd, sido_cd)}"
            )
            page_no = 1
            total_count = None

            while True:
                params = {
                    "serviceKey": self.service_key,
                    "stYy": st_yy,
                    "sidoCd": sido_cd,
                    "pageNo": page_no,
                    "numOfRows": self.DEFAULT_NUM_OF_ROWS,
                    "_type": "json",
                }

                response = self._make_request(url=base_url, params=params)

                if response is None:
                    self.logger.warning(
                        f"약품처방통계 요청 실패: sido_cd={sido_cd}, page={page_no}"
                    )
                    break

                data = self._parse_json_response(response)

                if data is None:
                    self.logger.warning(
                        f"약품처방통계 JSON 파싱 실패: sido_cd={sido_cd}"
                    )
                    break

                items = self._extract_items_from_response(data)

                if items is None:
                    result_code = self._get_result_code(data)
                    if result_code != "00":
                        self.logger.warning(
                            f"API 응답 에러: resultCode={result_code}"
                        )
                    break

                # 전체 건수 확인 (첫 페이지에서만)
                if total_count is None:
                    total_count = self._get_total_count(data)
                    self.logger.debug(
                        f"약품처방통계 전체 건수: {total_count} (sido_cd={sido_cd})"
                    )

                for item in items:
                    processed_item = self._process_drug_prsc_item(item)
                    all_data.append(processed_item)

                if len(items) < self.DEFAULT_NUM_OF_ROWS:
                    break

                page_no += 1
                time.sleep(0.1)  # API 호출 간격 조절

        return all_data

    def _extract_items_from_response(
        self,
        data: Dict[str, Any]
    ) -> Optional[List[Dict[str, Any]]]:
        """
        API 응답에서 items 추출

        Args:
            data: JSON 응답 데이터

        Returns:
            items 리스트 또는 None
        """
        try:
            response = data.get("response", {})
            body = response.get("body", {})
            items = body.get("items", {})

            # items가 빈 문자열인 경우 (데이터 없음)
            if items == "" or items is None:
                return None

            # items.item이 리스트인 경우
            item_list = items.get("item", [])

            # 단일 항목인 경우 리스트로 변환
            if isinstance(item_list, dict):
                return [item_list]

            return item_list if item_list else None

        except (AttributeError, TypeError) as e:
            self.logger.error(f"응답 파싱 중 오류: {e}")
            return None

    def _get_result_code(self, data: Dict[str, Any]) -> str:
        """
        API 응답에서 resultCode 추출

        Args:
            data: JSON 응답 데이터

        Returns:
            resultCode 문자열
        """
        try:
            return data.get("response", {}).get("header", {}).get(
                "resultCode", ""
            )
        except (AttributeError, TypeError):
            return ""

    def _get_total_count(self, data: Dict[str, Any]) -> int:
        """
        API 응답에서 전체 건수 추출

        Args:
            data: JSON 응답 데이터

        Returns:
            전체 건수
        """
        try:
            return int(
                data.get("response", {}).get("body", {}).get("totalCount", 0)
            )
        except (AttributeError, TypeError, ValueError):
            return 0

    def _process_medical_cost_item(
        self,
        item: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        의료비통계 항목 데이터 가공

        Args:
            item: 원본 API 응답 항목

        Returns:
            가공된 데이터
        """
        return {
            "stat_type": "medical_cost",
            "st_yy": item.get("stYy"),  # 기준년도
            "sido_cd": item.get("sidoCd"),  # 시도코드
            "sido_cd_nm": item.get("sidoCdNm"),  # 시도명
            "sggu_cd": item.get("sgguCd"),  # 시군구코드
            "sggu_cd_nm": item.get("sgguCdNm"),  # 시군구명
            "pat_cnt": self._safe_int(item.get("patCnt")),  # 환자수
            "rcpt_cnt": self._safe_int(item.get("rcptCnt")),  # 청구건수
            "mdc_amt_sum": self._safe_float(item.get("mdcAmtSum")),  # 의료비총액
            "mdc_amt_avg": self._safe_float(item.get("mdcAmtAvg")),  # 의료비평균
            "ins_amt_sum": self._safe_float(item.get("insAmtSum")),  # 보험자부담금총액
            "ins_amt_avg": self._safe_float(item.get("insAmtAvg")),  # 보험자부담금평균
            "own_amt_sum": self._safe_float(item.get("ownAmtSum")),  # 본인부담금총액
            "own_amt_avg": self._safe_float(item.get("ownAmtAvg")),  # 본인부담금평균
            "collected_at": datetime.now().isoformat(),
        }

    def _process_drug_prsc_item(
        self,
        item: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        약품처방통계 항목 데이터 가공

        Args:
            item: 원본 API 응답 항목

        Returns:
            가공된 데이터
        """
        return {
            "stat_type": "drug_prsc",
            "st_yy": item.get("stYy"),  # 기준년도
            "sido_cd": item.get("sidoCd"),  # 시도코드
            "sido_cd_nm": item.get("sidoCdNm"),  # 시도명
            "sggu_cd": item.get("sgguCd"),  # 시군구코드
            "sggu_cd_nm": item.get("sgguCdNm"),  # 시군구명
            "prsc_cnt": self._safe_int(item.get("prscCnt")),  # 처방건수
            "prsc_amt": self._safe_float(item.get("prscAmt")),  # 처방금액
            "drug_cnt": self._safe_int(item.get("drugCnt")),  # 처방약품수
            "drug_use_cnt": self._safe_int(item.get("drugUseCnt")),  # 처방사용량
            "collected_at": datetime.now().isoformat(),
        }

    def _safe_int(self, value: Any) -> Optional[int]:
        """
        안전한 정수 변환

        Args:
            value: 변환할 값

        Returns:
            정수 또는 None
        """
        if value is None or value == "":
            return None
        try:
            return int(value)
        except (ValueError, TypeError):
            return None

    def _safe_float(self, value: Any) -> Optional[float]:
        """
        안전한 실수 변환

        Args:
            value: 변환할 값

        Returns:
            실수 또는 None
        """
        if value is None or value == "":
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None

    def collect_and_save(
        self,
        st_yy: Optional[str] = None,
        sido_cd: Optional[str] = None,
        collect_medical_cost: bool = True,
        collect_drug_prsc: bool = True,
    ) -> Dict[str, int]:
        """
        데이터 수집 후 Elasticsearch에 저장

        Args:
            st_yy: 기준년도
            sido_cd: 시도코드
            collect_medical_cost: 의료비통계 수집 여부
            collect_drug_prsc: 약품처방통계 수집 여부

        Returns:
            저장 결과 통계
        """
        data = self.collect(
            st_yy=st_yy,
            sido_cd=sido_cd,
            collect_medical_cost=collect_medical_cost,
            collect_drug_prsc=collect_drug_prsc,
        )

        if not data:
            return {"success": 0, "failed": 0}

        # 문서 ID 생성 (기준년도_시도코드_시군구코드_통계유형)
        for item in data:
            item["_doc_id"] = (
                f"{item.get('st_yy', 'NA')}_"
                f"{item.get('sido_cd', 'NA')}_"
                f"{item.get('sggu_cd', 'NA')}_"
                f"{item.get('stat_type', 'NA')}"
            )

        return self.save_to_es(
            data=data,
            index_name=self.es_index,
            id_field="_doc_id"
        )


# 모듈 직접 실행 시 테스트
if __name__ == "__main__":
    collector = HealthStatCollector()

    # 테스트: 서울 지역 2023년 통계 수집
    data = collector.collect(
        st_yy="2023",
        sido_cd="11",  # 서울
        collect_medical_cost=True,
        collect_drug_prsc=True,
    )

    print(f"수집된 데이터 수: {len(data)}")
    if data:
        print("첫 번째 데이터 샘플:")
        print(data[0])
