"""
국립중앙의료원 병원 정보 수집기

전국 병·의원 찾기 서비스 및 응급의료정보 서비스에서 병원 데이터를 수집합니다.

API 정보:
- 서비스명: 전국 병·의원 찾기 서비스 / 중앙응급의료센터 응급의료정보 서비스
- 공공데이터 PK: 15000736 (병원), 15000563 (응급)
- Base URL: http://apis.data.go.kr/B552657/HsptlMdcncListInfoInqireService
- 응급 API: http://apis.data.go.kr/B552657/ErmctInfoInqireService
"""

from typing import Any, Dict, List, Optional
from datetime import datetime

from crawler.base_collector import BaseCollector
from crawler.config import config, CONFIG

# SIDO_CODES: (전체명, 짧은명) 튜플 리스트
SIDO_CODES = [
    ("서울특별시", "서울"), ("부산광역시", "부산"), ("대구광역시", "대구"),
    ("인천광역시", "인천"), ("광주광역시", "광주"), ("대전광역시", "대전"),
    ("울산광역시", "울산"), ("세종특별자치시", "세종"), ("경기도", "경기"),
    ("강원특별자치도", "강원"), ("충청북도", "충북"), ("충청남도", "충남"),
    ("전북특별자치도", "전북"), ("전라남도", "전남"), ("경상북도", "경북"),
    ("경상남도", "경남"), ("제주특별자치도", "제주"),
]

ES_INDICES = {
    "hospital_nic": "hospital_nic",
}


class HospitalNICCollector(BaseCollector):
    """
    국립중앙의료원 병원 정보 수집기
    
    수집 데이터:
    - 전국 병·의원 목록 (병원명, 주소, 전화번호, 좌표 등)
    - 응급의료기관 정보 (응급실 전화, 가용 병상 등)
    """

    BASE_URL = "http://apis.data.go.kr/B552657/HsptlMdcncListInfoInqireService"
    EMERGENCY_BASE_URL = "http://apis.data.go.kr/B552657/ErmctInfoInqireService"
    ENDPOINT_LIST = "/getHsptlMdcncListInfoInqire"
    ENDPOINT_EMERGENCY_LIST = "/getEgytListInfoInqire"
    ENDPOINT_EMERGENCY_REALTIME = "/getEmrrmRltmUsefulSckbdInfoInqire"

    def __init__(self):
        super().__init__(
            name="HospitalNICCollector",
            index_name=ES_INDICES.get("hospital_nic", "hospital_nic")
        )
        self._error_count = 0
        self._collected_count = 0

    def collect(self) -> List[Dict[str, Any]]:
        """
        전국 병원 정보 수집
        
        시도별로 순차적으로 수집하여 전체 병원 목록을 반환합니다.
        
        Returns:
            수집된 병원 데이터 목록
        """
        all_hospitals = []
        
        self.logger.info("병원 정보 수집 시작")
        
        for sido_full, sido_short in SIDO_CODES:
            self.logger.info(f"[{sido_short}] 수집 시작")
            
            hospitals = self._collect_by_sido(sido_short)
            all_hospitals.extend(hospitals)
            
            self.logger.info(f"[{sido_short}] {len(hospitals)}건 수집 완료")
        
        self.logger.info("응급의료기관 정보 수집 시작")
        emergency_info = self._collect_emergency_info()
        all_hospitals = self._merge_emergency_info(all_hospitals, emergency_info)
        
        self._collected_count = len(all_hospitals)
        self.logger.info(f"총 {self._collected_count}건 수집 완료")
        
        return all_hospitals

    def _collect_by_sido(
        self,
        sido: str,
        sigungu: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        시도/시군구별 병원 목록 수집
        
        Args:
            sido: 시도명 (예: "서울", "경기")
            sigungu: 시군구명 (선택)
            
        Returns:
            해당 지역 병원 목록
        """
        hospitals = []
        url = f"{self.BASE_URL}{self.ENDPOINT_LIST}"
        
        base_params = {
            "serviceKey": self.service_key,
            "Q0": sido,
        }
        
        if sigungu:
            base_params["Q1"] = sigungu

        page_no = 1
        num_of_rows = 100

        while True:
            params = {**base_params, "pageNo": page_no, "numOfRows": num_of_rows}
            response = self._make_request(url, params=params)

            if response is None:
                break

            json_data = self._parse_json_response(response)
            if json_data is None:
                break

            items = self.extract_items(json_data)
            if not items:
                break

            for item in items:
                try:
                    transformed = self.transform(item)
                    if transformed:
                        hospitals.append(transformed)
                except Exception as e:
                    self.logger.warning(f"데이터 변환 오류: {e}")
                    self._error_count += 1

            total_count = self.extract_total_count(json_data)
            if total_count and page_no * num_of_rows >= total_count:
                break

            page_no += 1

        return hospitals

    def _collect_emergency_info(self) -> Dict[str, Dict[str, Any]]:
        """
        응급의료기관 정보 수집
        
        Returns:
            hpid를 키로 하는 응급의료기관 정보 딕셔너리
        """
        emergency_map = {}
        url = f"{self.EMERGENCY_BASE_URL}{self.ENDPOINT_EMERGENCY_LIST}"
        
        for sido_full, sido_short in SIDO_CODES:
            base_params = {
                "serviceKey": self.service_key,
                "STAGE1": sido_full,
            }

            page_no = 1
            num_of_rows = 100

            while True:
                params = {**base_params, "pageNo": page_no, "numOfRows": num_of_rows}
                response = self._make_request(url, params=params)

                if response is None:
                    break

                json_data = self._parse_json_response(response)
                if json_data is None:
                    break

                items = self.extract_items(json_data)
                if not items:
                    break

                for item in items:
                    hpid = item.get("hpid")
                    if hpid:
                        emergency_map[hpid] = {
                            "is_emergency": True,
                            "emergency_tel": item.get("dutyTel3"),
                            "emergency_room_available": item.get("hvec"),
                            "icu_available": item.get("hvcc"),
                            "emergency_grade": item.get("dgidIdName"),
                        }

                total_count = self.extract_total_count(json_data)
                if total_count and page_no * num_of_rows >= total_count:
                    break

                page_no += 1
        
        self.logger.info(f"응급의료기관 {len(emergency_map)}곳 정보 수집")
        return emergency_map

    def _merge_emergency_info(
        self,
        hospitals: List[Dict[str, Any]],
        emergency_info: Dict[str, Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        병원 목록에 응급의료기관 정보 병합
        
        Args:
            hospitals: 병원 목록
            emergency_info: 응급의료기관 정보 딕셔너리
            
        Returns:
            응급정보가 병합된 병원 목록
        """
        for hospital in hospitals:
            hpid = hospital.get("hpid")
            if hpid and hpid in emergency_info:
                hospital.update(emergency_info[hpid])
            else:
                hospital["is_emergency"] = False
        
        return hospitals

    def transform(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        API 응답 데이터를 정제된 형식으로 변환
        
        Args:
            raw_data: API 응답 원본 데이터
            
        Returns:
            정제된 병원 데이터
        """
        if not raw_data:
            return {}

        # 좌표 변환 (문자열 -> 실수)
        lat = self._safe_float(raw_data.get("wgs84Lat"))
        lon = self._safe_float(raw_data.get("wgs84Lon"))
        
        # 운영시간 파싱
        operating_hours = self._parse_operating_hours(raw_data)

        return {
            # 기본 정보
            "hpid": raw_data.get("hpid"),
            "name": raw_data.get("dutyName"),
            "address": raw_data.get("dutyAddr"),
            "tel": raw_data.get("dutyTel1"),
            "fax": raw_data.get("dutyFax"),
            
            # 위치 정보
            "latitude": lat,
            "longitude": lon,
            "location": {
                "lat": lat,
                "lon": lon
            } if lat and lon else None,
            
            # 우편번호
            "postal_code": self._format_postal_code(
                raw_data.get("postCdn1"),
                raw_data.get("postCdn2")
            ),
            
            # 운영 정보
            "operating_hours": operating_hours,
            
            # 진료 정보
            "duty_div": raw_data.get("dutyDiv"),
            "duty_div_name": raw_data.get("dutyDivNam"),
            "duty_emcls": raw_data.get("dutyEmcls"),
            "duty_emcls_name": raw_data.get("dutyEmclsName"),
            
            # 기타 정보
            "duty_inf": raw_data.get("dutyInf"),
            "duty_mapimg": raw_data.get("dutyMapimg"),
            "duty_eryn": raw_data.get("dutyEryn") == "1",  # 응급실 운영 여부
            
            # 메타 정보
            "source": "NIC",  # National Information Center (국립중앙의료원)
            "collected_at": datetime.now().isoformat(),
            "raw_data": raw_data,  # 원본 데이터 보존
        }

    def _parse_operating_hours(self, data: Dict[str, Any]) -> Dict[str, Dict[str, str]]:
        """
        운영시간 데이터 파싱
        
        Args:
            data: API 응답 데이터
            
        Returns:
            요일별 운영시간 딕셔너리
        """
        days = {
            "1": "monday",
            "2": "tuesday", 
            "3": "wednesday",
            "4": "thursday",
            "5": "friday",
            "6": "saturday",
            "7": "sunday",
            "8": "holiday",
        }
        
        hours = {}
        
        for num, day_name in days.items():
            start_key = f"dutyTime{num}s"
            close_key = f"dutyTime{num}c"
            
            start = data.get(start_key)
            close = data.get(close_key)
            
            if start or close:
                hours[day_name] = {
                    "open": self._format_time(start),
                    "close": self._format_time(close),
                }
        
        return hours

    def _format_time(self, time_str: Optional[str]) -> Optional[str]:
        """
        시간 문자열 포맷팅 (HHMM -> HH:MM)
        
        Args:
            time_str: 시간 문자열 (예: "0900")
            
        Returns:
            포맷된 시간 문자열 (예: "09:00")
        """
        if not time_str or len(time_str) < 4:
            return None
            
        try:
            return f"{time_str[:2]}:{time_str[2:4]}"
        except Exception:
            return time_str

    def _format_postal_code(
        self,
        code1: Optional[str],
        code2: Optional[str]
    ) -> Optional[str]:
        """
        우편번호 포맷팅
        
        Args:
            code1: 우편번호 앞자리
            code2: 우편번호 뒷자리
            
        Returns:
            포맷된 우편번호 또는 None
        """
        if code1 and code2:
            return f"{code1}-{code2}"
        elif code1:
            return code1
        return None

    def _safe_float(self, value: Any) -> Optional[float]:
        """
        안전한 실수 변환
        
        Args:
            value: 변환할 값
            
        Returns:
            실수 또는 None
        """
        if value is None:
            return None
            
        try:
            return float(value)
        except (ValueError, TypeError):
            return None

    def collect_by_region(
        self,
        sido: str,
        sigungu: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        특정 지역 병원 정보 수집 (외부 호출용)
        
        Args:
            sido: 시도명
            sigungu: 시군구명 (선택)
            
        Returns:
            해당 지역 병원 목록
        """
        return self._collect_by_sido(sido, sigungu)

    def collect_emergency_only(self) -> List[Dict[str, Any]]:
        """
        응급의료기관만 수집
        
        Returns:
            응급의료기관 목록
        """
        all_hospitals = self.collect()
        return [h for h in all_hospitals if h.get("is_emergency")]
