"""
국립중앙의료원 약국 정보 수집기

공공데이터포털의 국립중앙의료원 약국 정보 API를 통해
전국 약국 데이터를 수집합니다.

API 문서: https://www.data.go.kr/data/15000563/openapi.do
"""

import time
import xml.etree.ElementTree as ET
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from crawler.base_collector import BaseCollector
from crawler.config import CONFIG


class PharmacyNICCollector(BaseCollector):
    """
    국립중앙의료원 약국 정보 수집기

    Attributes:
        BASE_URL: API 기본 URL
        ENDPOINT: 약국 목록 조회 엔드포인트
        ES_INDEX: Elasticsearch 인덱스명
        DEFAULT_NUM_OF_ROWS: 한 페이지당 기본 항목 수
    """

    BASE_URL = "http://apis.data.go.kr/B552657/ErmctInsttInfoInqireService"
    ENDPOINT = "/getParmacyListInfoInqire"
    ES_INDEX = "pharmacy_nic"
    DEFAULT_NUM_OF_ROWS = 100

    # 전국 시도 목록 (Q0 파라미터용)
    SIDO_LIST = [
        "서울특별시",
        "부산광역시",
        "대구광역시",
        "인천광역시",
        "광주광역시",
        "대전광역시",
        "울산광역시",
        "세종특별자치시",
        "경기도",
        "강원특별자치도",
        "충청북도",
        "충청남도",
        "전북특별자치도",
        "전라남도",
        "경상북도",
        "경상남도",
        "제주특별자치도",
    ]

    def __init__(self):
        """PharmacyNICCollector 초기화"""
        super().__init__(name="PharmacyNICCollector")
        self.service_key = CONFIG["data_go_kr"]["service_key"]

        if not self.service_key:
            self.logger.warning(
                "DATA_GO_KR_API_KEY가 설정되지 않았습니다. "
                ".env 파일을 확인해주세요."
            )

    def _build_request_url(
        self,
        sido: str,
        sigungu: Optional[str] = None,
        page_no: int = 1,
        num_of_rows: int = DEFAULT_NUM_OF_ROWS,
    ) -> str:
        """
        API 요청 URL 생성

        Args:
            sido: 시도명 (Q0)
            sigungu: 시군구명 (Q1, 선택)
            page_no: 페이지 번호
            num_of_rows: 한 페이지당 항목 수

        Returns:
            완성된 요청 URL
        """
        return f"{self.BASE_URL}{self.ENDPOINT}"

    def _build_request_params(
        self,
        sido: str,
        sigungu: Optional[str] = None,
        page_no: int = 1,
        num_of_rows: int = DEFAULT_NUM_OF_ROWS,
    ) -> Dict[str, Any]:
        """
        API 요청 파라미터 생성

        Args:
            sido: 시도명 (Q0)
            sigungu: 시군구명 (Q1, 선택)
            page_no: 페이지 번호
            num_of_rows: 한 페이지당 항목 수

        Returns:
            요청 파라미터 딕셔너리
        """
        params = {
            "serviceKey": self.service_key,
            "pageNo": page_no,
            "numOfRows": num_of_rows,
            "Q0": sido,
        }

        if sigungu:
            params["Q1"] = sigungu

        return params

    def _parse_xml_item(self, item_element: ET.Element) -> Dict[str, Any]:
        """
        XML item 요소를 딕셔너리로 파싱

        Args:
            item_element: XML item 요소

        Returns:
            파싱된 데이터 딕셔너리
        """
        data = {}
        for child in item_element:
            tag = child.tag
            text = child.text.strip() if child.text else None
            data[tag] = text

        return data

    def _parse_response(
        self,
        xml_text: str
    ) -> Tuple[List[Dict[str, Any]], int]:
        """
        API XML 응답 파싱

        Args:
            xml_text: XML 응답 텍스트

        Returns:
            (파싱된 항목 리스트, 전체 항목 수) 튜플
        """
        items = []
        total_count = 0

        try:
            root = ET.fromstring(xml_text)

            # 결과 코드 확인
            result_code = root.find(".//resultCode")
            if result_code is not None and result_code.text != "00":
                result_msg = root.find(".//resultMsg")
                msg = result_msg.text if result_msg is not None else "Unknown error"
                self.logger.error(f"API 오류: {msg}")
                return items, total_count

            # 전체 건수 조회
            total_count_elem = root.find(".//totalCount")
            if total_count_elem is not None and total_count_elem.text:
                total_count = int(total_count_elem.text)

            # items 파싱
            item_elements = root.findall(".//item")
            for item_elem in item_elements:
                item_data = self._parse_xml_item(item_elem)
                items.append(item_data)

        except ET.ParseError as e:
            self.logger.error(f"XML 파싱 오류: {e}")
        except Exception as e:
            self.logger.error(f"응답 파싱 중 오류 발생: {e}")

        return items, total_count

    def _transform_item(self, raw_item: Dict[str, Any]) -> Dict[str, Any]:
        """
        수집된 원본 데이터를 ES 저장용 포맷으로 변환

        Args:
            raw_item: 원본 데이터

        Returns:
            변환된 데이터 (ES 저장용)
        """
        # 위경도 변환 (문자열 -> float)
        lat = raw_item.get("wgs84Lat")
        lon = raw_item.get("wgs84Lon")

        try:
            lat = float(lat) if lat else None
        except (ValueError, TypeError):
            lat = None

        try:
            lon = float(lon) if lon else None
        except (ValueError, TypeError):
            lon = None

        # geo_point 형식으로 위치 정보 생성
        location = None
        if lat is not None and lon is not None:
            location = {
                "lat": lat,
                "lon": lon
            }

        return {
            # 기본 식별 정보
            "hpid": raw_item.get("hpid"),  # 약국 ID (PK)
            "duty_name": raw_item.get("dutyName"),  # 약국명
            "duty_addr": raw_item.get("dutyAddr"),  # 주소
            "duty_tel1": raw_item.get("dutyTel1"),  # 전화번호

            # 위치 정보
            "wgs84_lat": lat,
            "wgs84_lon": lon,
            "location": location,  # ES geo_point용

            # 운영 시간 정보
            "duty_time1s": raw_item.get("dutyTime1s"),  # 월요일 시작
            "duty_time1c": raw_item.get("dutyTime1c"),  # 월요일 종료
            "duty_time2s": raw_item.get("dutyTime2s"),  # 화요일 시작
            "duty_time2c": raw_item.get("dutyTime2c"),  # 화요일 종료
            "duty_time3s": raw_item.get("dutyTime3s"),  # 수요일 시작
            "duty_time3c": raw_item.get("dutyTime3c"),  # 수요일 종료
            "duty_time4s": raw_item.get("dutyTime4s"),  # 목요일 시작
            "duty_time4c": raw_item.get("dutyTime4c"),  # 목요일 종료
            "duty_time5s": raw_item.get("dutyTime5s"),  # 금요일 시작
            "duty_time5c": raw_item.get("dutyTime5c"),  # 금요일 종료
            "duty_time6s": raw_item.get("dutyTime6s"),  # 토요일 시작
            "duty_time6c": raw_item.get("dutyTime6c"),  # 토요일 종료
            "duty_time7s": raw_item.get("dutyTime7s"),  # 일요일 시작
            "duty_time7c": raw_item.get("dutyTime7c"),  # 일요일 종료
            "duty_time8s": raw_item.get("dutyTime8s"),  # 공휴일 시작
            "duty_time8c": raw_item.get("dutyTime8c"),  # 공휴일 종료

            # 추가 정보
            "post_cdn1": raw_item.get("postCdn1"),  # 우편번호1
            "post_cdn2": raw_item.get("postCdn2"),  # 우편번호2
            "duty_div_nam": raw_item.get("dutyDivNam"),  # 약국 분류명
            "duty_mapimg": raw_item.get("dutyMapimg"),  # 지도 이미지
            "duty_inf": raw_item.get("dutyInf"),  # 기타 정보

            # 메타데이터
            "collected_at": datetime.utcnow().isoformat(),
            "source": "NIC",  # 국립중앙의료원
        }

    def _collect_by_sido(self, sido: str) -> List[Dict[str, Any]]:
        """
        특정 시도의 모든 약국 데이터 수집

        Args:
            sido: 시도명

        Returns:
            수집된 약국 데이터 리스트
        """
        all_items = []
        page_no = 1
        total_count = None

        self.logger.info(f"[{sido}] 수집 시작...")

        while True:
            url = self._build_request_url(sido, page_no=page_no)
            params = self._build_request_params(sido, page_no=page_no)

            response = self._make_request(url, params=params, max_retries=3)

            if response is None:
                self.logger.error(f"[{sido}] 페이지 {page_no} 요청 실패")
                break

            xml_text = self._parse_xml_response(response)
            if xml_text is None:
                self.logger.error(f"[{sido}] 페이지 {page_no} XML 파싱 실패")
                break

            items, count = self._parse_response(xml_text)

            # 첫 페이지에서 총 건수 저장
            if total_count is None:
                total_count = count
                self.logger.info(f"[{sido}] 총 {total_count}건 조회 예정")

            if not items:
                break

            all_items.extend(items)
            self.logger.debug(
                f"[{sido}] 페이지 {page_no} 수집 완료: {len(items)}건 "
                f"(누적: {len(all_items)}/{total_count}건)"
            )

            # 다음 페이지 확인
            if len(all_items) >= total_count:
                break

            page_no += 1

            # API 호출 간격 조절 (Rate limiting 방지)
            time.sleep(0.1)

        self.logger.info(f"[{sido}] 수집 완료: 총 {len(all_items)}건")
        return all_items

    def collect(self) -> List[Dict[str, Any]]:
        """
        전국 약국 데이터 수집

        Returns:
            수집된 모든 약국 데이터 리스트
        """
        if not self.service_key:
            self.logger.error("API 키가 설정되지 않아 수집을 중단합니다.")
            return []

        all_pharmacies = []

        self.logger.info("=" * 50)
        self.logger.info("국립중앙의료원 약국 데이터 수집 시작")
        self.logger.info(f"대상 시도: {len(self.SIDO_LIST)}개")
        self.logger.info("=" * 50)

        for sido in self.SIDO_LIST:
            try:
                items = self._collect_by_sido(sido)

                # 데이터 변환
                transformed_items = [
                    self._transform_item(item) for item in items
                ]
                all_pharmacies.extend(transformed_items)

                # 시도별 수집 후 짧은 대기
                time.sleep(0.5)

            except Exception as e:
                self.logger.error(f"[{sido}] 수집 중 오류 발생: {e}")
                continue

        self.logger.info("=" * 50)
        self.logger.info(f"전체 수집 완료: 총 {len(all_pharmacies)}건")
        self.logger.info("=" * 50)

        return all_pharmacies

    def run(self) -> Dict[str, Any]:
        """
        수집기 실행 (수집 -> ES 저장)

        Returns:
            실행 결과 통계
        """
        start_time = datetime.now()

        # 데이터 수집
        data = self.collect()

        # Elasticsearch 저장
        save_result = self.save_to_es(
            data=data,
            index_name=self.ES_INDEX,
            id_field="hpid"  # 약국 ID를 문서 ID로 사용
        )

        end_time = datetime.now()
        elapsed_time = (end_time - start_time).total_seconds()

        result = {
            "collector": self.name,
            "index": self.ES_INDEX,
            "total_collected": len(data),
            "es_success": save_result["success"],
            "es_failed": save_result["failed"],
            "elapsed_seconds": elapsed_time,
            "started_at": start_time.isoformat(),
            "finished_at": end_time.isoformat(),
        }

        self.logger.info(f"실행 결과: {result}")
        return result


# 직접 실행 시
if __name__ == "__main__":
    collector = PharmacyNICCollector()
    result = collector.run()
    print(f"\n수집 결과: {result}")
