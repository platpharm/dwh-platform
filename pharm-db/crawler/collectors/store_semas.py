"""
소상공인시장진흥공단 상가(상권)정보 수집기

데이터 소스: 소상공인시장진흥공단_상가(상권)정보
공공데이터 PK: 15012005
Base URL: https://apis.data.go.kr/B553077/api/open/sdsc2
엔드포인트: /storeListInDong (행정동별 상가)

약국 관련 업종으로 필터링하여 수집합니다.
"""

import os
from typing import Any, Dict, List, Optional

from crawler.base_collector import BaseCollector
from crawler.config import CONFIG


class StoreSEMASCollector(BaseCollector):
    """
    소상공인시장진흥공단 상가 정보 수집기

    행정동별로 상가(약국/의료건강 관련) 정보를 수집합니다.
    """

    ES_INDEX = "store_semas"
    BASE_URL = "https://apis.data.go.kr/B553077/api/open/sdsc2"
    DEFAULT_NUM_OF_ROWS = 1000

    CTPRVN_CODES = [
        "11", "21", "22", "23", "24", "25", "26", "29",
        "31", "32", "33", "34", "35", "36", "37", "38", "39",
    ]

    CTPRVN_NAMES = {
        "11": "서울", "21": "부산", "22": "대구", "23": "인천",
        "24": "광주", "25": "대전", "26": "울산", "29": "세종",
        "31": "경기", "32": "강원", "33": "충북", "34": "충남",
        "35": "전북", "36": "전남", "37": "경북", "38": "경남",
        "39": "제주",
    }

    PHARMACY_INDUSTRY_CODES = {
        "large": ["Q1", "G2"],
        "medium": ["Q101", "Q102", "Q103", "Q104", "G215"],
        "small": [],
    }

    COLLECT_TARGETS = [
        {"divId": "indsLclsCd", "key": "Q1", "label": "보건의료"},
        {"divId": "indsMclsCd", "key": "G215", "label": "의약·화장품 소매"},
    ]

    def __init__(self):
        """StoreSEMASCollector 초기화"""
        super().__init__(name="StoreSEMASCollector")
        self.service_key = CONFIG["data_go_kr"]["service_key"]

    def _get_store_list_in_dong(
        self,
        div_id: str,
        inds_lcls_cd: Optional[str] = None,
        inds_mcls_cd: Optional[str] = None,
        inds_scls_cd: Optional[str] = None,
        page_no: int = 1,
        num_of_rows: int = None
    ) -> Optional[Dict[str, Any]]:
        """
        행정동별 상가 목록 조회

        Args:
            div_id: 행정동 코드
            inds_lcls_cd: 대분류 업종코드 (선택)
            inds_mcls_cd: 중분류 업종코드 (선택)
            inds_scls_cd: 소분류 업종코드 (선택)
            page_no: 페이지 번호
            num_of_rows: 페이지당 결과 수

        Returns:
            API 응답 데이터 또는 None
        """
        url = f"{self.BASE_URL}/storeListInDong"

        params = {
            "serviceKey": self.service_key,
            "divId": div_id,
            "pageNo": page_no,
            "numOfRows": num_of_rows or self.DEFAULT_NUM_OF_ROWS,
            "type": "json"
        }

        if inds_lcls_cd:
            params["indsLclsCd"] = inds_lcls_cd
        if inds_mcls_cd:
            params["indsMclsCd"] = inds_mcls_cd
        if inds_scls_cd:
            params["indsSclsCd"] = inds_scls_cd

        response = self._make_request(url, params=params)

        if response is None:
            return None

        json_data = self._parse_json_response(response)
        if json_data is None:
            return None

        return json_data

    def _get_store_list_in_radius(
        self,
        cx: float,
        cy: float,
        radius: int = 500,
        inds_lcls_cd: Optional[str] = None,
        inds_mcls_cd: Optional[str] = None,
        inds_scls_cd: Optional[str] = None,
        page_no: int = 1,
        num_of_rows: int = None
    ) -> Optional[Dict[str, Any]]:
        """
        반경 내 상가 목록 조회

        Args:
            cx: 중심점 경도 (WGS84)
            cy: 중심점 위도 (WGS84)
            radius: 반경 (미터, 최대 500)
            inds_lcls_cd: 대분류 업종코드 (선택)
            inds_mcls_cd: 중분류 업종코드 (선택)
            inds_scls_cd: 소분류 업종코드 (선택)
            page_no: 페이지 번호
            num_of_rows: 페이지당 결과 수

        Returns:
            API 응답 데이터 또는 None
        """
        url = f"{self.BASE_URL}/storeListInRadius"

        params = {
            "serviceKey": self.service_key,
            "cx": cx,
            "cy": cy,
            "radius": min(radius, 500),  # 최대 500m 제한
            "pageNo": page_no,
            "numOfRows": num_of_rows or self.DEFAULT_NUM_OF_ROWS,
            "type": "json"
        }

        if inds_lcls_cd:
            params["indsLclsCd"] = inds_lcls_cd
        if inds_mcls_cd:
            params["indsMclsCd"] = inds_mcls_cd
        if inds_scls_cd:
            params["indsSclsCd"] = inds_scls_cd

        response = self._make_request(url, params=params)

        if response is None:
            return None

        json_data = self._parse_json_response(response)
        if json_data is None:
            return None

        return json_data

    def _get_store_one(self, bizes_id: str) -> Optional[Dict[str, Any]]:
        """
        단일 상가 상세 정보 조회

        Args:
            bizes_id: 상가업소번호

        Returns:
            상가 상세 정보 또는 None
        """
        url = f"{self.BASE_URL}/storeOne"

        params = {
            "serviceKey": self.service_key,
            "key": bizes_id,
            "type": "json"
        }

        response = self._make_request(url, params=params)

        if response is None:
            return None

        json_data = self._parse_json_response(response)
        if json_data is None:
            return None

        return json_data

    def _get_industry_codes(
        self,
        level: str = "large",
        parent_code: Optional[str] = None
    ) -> Optional[List[Dict[str, Any]]]:
        """
        업종 코드 목록 조회

        Args:
            level: 업종 수준 ('large', 'medium', 'small')
            parent_code: 상위 업종 코드 (중/소분류 조회 시 필요)

        Returns:
            업종 코드 목록 또는 None
        """
        endpoints = {
            "large": "/largeUpjongList",
            "medium": "/middleUpjongList",
            "small": "/smallUpjongList"
        }

        url = f"{self.BASE_URL}{endpoints.get(level, '/largeUpjongList')}"

        params = {
            "serviceKey": self.service_key,
            "type": "json"
        }

        if parent_code:
            if level == "medium":
                params["indsLclsCd"] = parent_code
            elif level == "small":
                params["indsMclsCd"] = parent_code

        response = self._make_request(url, params=params)

        if response is None:
            return None

        json_data = self._parse_json_response(response)
        if json_data is None:
            return None

        try:
            body = json_data.get("body", {})
            items = body.get("items", [])
            return items if isinstance(items, list) else []
        except (KeyError, TypeError) as e:
            self.logger.error(f"업종 코드 파싱 오류: {e}")
            return []

    def _extract_items_from_response(
        self,
        response_data: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """
        API 응답에서 상가 목록 추출

        Args:
            response_data: API 응답 데이터

        Returns:
            상가 목록
        """
        try:
            body = response_data.get("body", {})
            items = body.get("items", [])

            if not isinstance(items, list):
                return []

            return items

        except (KeyError, TypeError) as e:
            self.logger.error(f"응답 데이터 파싱 오류: {e}")
            return []

    def _get_total_count(self, response_data: Dict[str, Any]) -> int:
        """
        API 응답에서 전체 결과 수 추출

        Args:
            response_data: API 응답 데이터

        Returns:
            전체 결과 수
        """
        try:
            body = response_data.get("body", {})
            return int(body.get("totalCount", 0))
        except (KeyError, TypeError, ValueError):
            return 0

    def _is_pharmacy_related(self, item: Dict[str, Any]) -> bool:
        """
        상가가 약국/의료건강 관련인지 확인

        Args:
            item: 상가 정보

        Returns:
            약국/의료건강 관련 여부
        """
        inds_lcls_cd = item.get("indsLclsCd", "")
        inds_mcls_cd = item.get("indsMclsCd", "")

        if inds_lcls_cd in self.PHARMACY_INDUSTRY_CODES["large"]:
            return True

        if inds_mcls_cd in self.PHARMACY_INDUSTRY_CODES["medium"]:
            return True

        bizes_nm = item.get("bizesNm", "")
        if "약국" in bizes_nm:
            return True

        return False

    def _normalize_item(self, item: Dict[str, Any]) -> Dict[str, Any]:
        """
        상가 데이터 정규화

        Args:
            item: 원본 상가 정보

        Returns:
            정규화된 상가 정보
        """
        return {
            # 기본 정보
            "bizesId": item.get("bizesId"),
            "bizesNm": item.get("bizesNm"),
            "brchNm": item.get("brchNm"),

            # 업종 분류
            "indsLclsCd": item.get("indsLclsCd"),
            "indsLclsNm": item.get("indsLclsNm"),
            "indsMclsCd": item.get("indsMclsCd"),
            "indsMclsNm": item.get("indsMclsNm"),
            "indsSclsCd": item.get("indsSclsCd"),
            "indsSclsNm": item.get("indsSclsNm"),

            # 표준산업분류
            "ksicCd": item.get("ksicCd"),
            "ksicNm": item.get("ksicNm"),

            # 지역 정보
            "ctprvnCd": item.get("ctprvnCd"),
            "ctprvnNm": item.get("ctprvnNm"),
            "signguCd": item.get("signguCd"),
            "signguNm": item.get("signguNm"),
            "adongCd": item.get("adongCd"),
            "adongNm": item.get("adongNm"),
            "ldongCd": item.get("ldongCd"),
            "ldongNm": item.get("ldongNm"),

            # 주소 정보
            "lnoAdr": item.get("lnoAdr"),
            "rdnmAdr": item.get("rdnmAdr"),
            "bldNm": item.get("bldNm"),
            "bldMngNo": item.get("bldMngNo"),
            "flrNo": item.get("flrNo"),

            # 좌표 정보
            "lon": item.get("lon"),
            "lat": item.get("lat"),

            # 메타데이터
            "source": "SEMAS",
            "esIndex": self.ES_INDEX
        }

    def collect_by_dong(
        self,
        dong_codes: List[str],
        filter_pharmacy: bool = True
    ) -> List[Dict[str, Any]]:
        """
        행정동 목록별로 상가 수집

        Args:
            dong_codes: 수집할 행정동 코드 목록
            filter_pharmacy: 약국/의료건강 관련만 필터링할지 여부

        Returns:
            수집된 상가 목록
        """
        all_items = []

        for dong_code in dong_codes:
            self.logger.info(f"행정동 수집 시작: {dong_code}")

            page_no = 1
            has_more = True

            while has_more:
                response_data = self._get_store_list_in_dong(
                    div_id=dong_code,
                    inds_mcls_cd="Q16" if filter_pharmacy else None,  # 의료/건강 중분류
                    page_no=page_no,
                    num_of_rows=self.DEFAULT_NUM_OF_ROWS
                )

                if response_data is None:
                    self.logger.warning(f"행정동 {dong_code} 데이터 조회 실패")
                    break

                items = self._extract_items_from_response(response_data)

                if not items:
                    has_more = False
                    continue

                for item in items:
                    if filter_pharmacy and not self._is_pharmacy_related(item):
                        continue
                    normalized = self._normalize_item(item)
                    all_items.append(normalized)

                total_count = self._get_total_count(response_data)
                current_count = page_no * self.DEFAULT_NUM_OF_ROWS

                if current_count >= total_count:
                    has_more = False
                else:
                    page_no += 1

            self.logger.info(
                f"행정동 {dong_code} 수집 완료: {len(all_items)}건"
            )

        return all_items

    def collect_by_radius(
        self,
        center_points: List[Dict[str, float]],
        radius: int = 500,
        filter_pharmacy: bool = True
    ) -> List[Dict[str, Any]]:
        """
        중심점 반경별로 상가 수집

        Args:
            center_points: 중심점 목록 [{"lon": 경도, "lat": 위도}, ...]
            radius: 반경 (미터)
            filter_pharmacy: 약국/의료건강 관련만 필터링할지 여부

        Returns:
            수집된 상가 목록
        """
        all_items = []
        collected_ids = set()  # 중복 방지용

        for point in center_points:
            lon = point.get("lon")
            lat = point.get("lat")

            if lon is None or lat is None:
                continue

            self.logger.info(f"반경 수집 시작: ({lon}, {lat}), 반경 {radius}m")

            page_no = 1
            has_more = True

            while has_more:
                response_data = self._get_store_list_in_radius(
                    cx=lon,
                    cy=lat,
                    radius=radius,
                    inds_mcls_cd="Q16" if filter_pharmacy else None,
                    page_no=page_no,
                    num_of_rows=self.DEFAULT_NUM_OF_ROWS
                )

                if response_data is None:
                    self.logger.warning(f"반경 ({lon}, {lat}) 데이터 조회 실패")
                    break

                items = self._extract_items_from_response(response_data)

                if not items:
                    has_more = False
                    continue

                for item in items:
                    bizes_id = item.get("bizesId")

                    if bizes_id in collected_ids:
                        continue

                    if filter_pharmacy and not self._is_pharmacy_related(item):
                        continue

                    normalized = self._normalize_item(item)
                    all_items.append(normalized)
                    collected_ids.add(bizes_id)

                total_count = self._get_total_count(response_data)
                current_count = page_no * self.DEFAULT_NUM_OF_ROWS

                if current_count >= total_count:
                    has_more = False
                else:
                    page_no += 1

        self.logger.info(f"반경 수집 완료: 총 {len(all_items)}건")
        return all_items

    def collect(self) -> List[Dict[str, Any]]:
        """
        전체 상가 데이터 수집

        storeListInUpjong API를 사용하여 보건의료(Q1) 및
        의약·화장품 소매(G215) 업종을 전국에서 수집합니다.

        Returns:
            수집된 상가 목록
        """
        self.logger.info("소상공인진흥공단 상가 데이터 수집 시작")

        all_items = []
        collected_ids = set()

        for target in self.COLLECT_TARGETS:
            label = target["label"]
            self.logger.info(f"[{label}] 수집 시작")

            target_items = self._collect_by_upjong(
                div_id=target["divId"],
                key=target["key"],
            )

            new_items = []
            for item in target_items:
                bizes_id = item.get("bizesId")
                if bizes_id and bizes_id not in collected_ids:
                    collected_ids.add(bizes_id)
                    new_items.append(item)

            all_items.extend(new_items)

            self.logger.info(
                f"[{label}] 수집 완료: {len(new_items)}건 "
                f"(누적: {len(all_items)}건)"
            )

        self.logger.info(f"전국 수집 완료: 총 {len(all_items)}건")
        return all_items

    def _collect_by_upjong(
        self,
        div_id: str,
        key: str,
    ) -> List[Dict[str, Any]]:
        """
        업종별 상가 수집 (storeListInUpjong)

        Args:
            div_id: 업종 구분 (indsLclsCd, indsMclsCd 등)
            key: 업종 코드

        Returns:
            수집된 상가 목록
        """
        items = []
        page_no = 1
        url = f"{self.BASE_URL}/storeListInUpjong"

        while True:
            params = {
                "serviceKey": self.service_key,
                "divId": div_id,
                "key": key,
                "pageNo": page_no,
                "numOfRows": self.DEFAULT_NUM_OF_ROWS,
                "type": "json",
            }

            response = self._make_request(url, params=params)

            if response is None:
                break

            response_data = self._parse_json_response(response)
            if response_data is None:
                break

            page_items = self._extract_items_from_response(response_data)

            if not page_items:
                break

            for item in page_items:
                normalized = self._normalize_item(item)
                items.append(normalized)

            total_count = self._get_total_count(response_data)
            current_count = page_no * self.DEFAULT_NUM_OF_ROWS

            self.logger.info(
                f"페이지 {page_no} 수집: {len(page_items)}건 "
                f"(누적: {len(items)}건, 전체: {total_count}건)"
            )

            if current_count >= total_count:
                break

            page_no += 1

        return items

    def run(self, save_to_es: bool = True) -> List[Dict[str, Any]]:
        """
        수집 실행 및 Elasticsearch 저장

        Args:
            save_to_es: Elasticsearch에 저장할지 여부

        Returns:
            수집된 상가 목록
        """
        data = self.collect()

        if save_to_es and data:
            self.save_to_es(
                data=data,
                index_name=self.ES_INDEX,
                id_field="bizesId"
            )

        return data


if __name__ == "__main__":
    import logging

    logging.basicConfig(level=logging.INFO)

    collector = StoreSEMASCollector()
    data = collector.collect()

    print(f"수집 결과: {len(data)}건")
    if data:
        print(f"샘플 데이터: {data[0]}")
