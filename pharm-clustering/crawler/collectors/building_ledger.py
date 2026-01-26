"""
국토교통부 건축물대장 정보 수집기 (Building Ledger Collector)

건축물대장 정보 서비스 API를 사용하여 건축물의 면적 정보를 수집합니다.
상가(소상공인시장진흥공단) 데이터의 bldMngNo와 연계하여 면적을 조회합니다.

API 정보:
    - Base URL: http://apis.data.go.kr/1613000/BldRgstHubService
    - 엔드포인트:
        - /getBrTitleInfo (총괄표제부 기본정보)
        - /getBrExposPubuseAreaInfo (전유공용면적 - 상가 면적 핵심)
    - 인증: data.go.kr API Key (serviceKey)
    - 파라미터: sigunguCd(5자리), bjdongCd(5자리), bun(4자리), ji(4자리)

ES 인덱스: building_ledger

면적 분류 기준:
    - 대형: 330㎡ 이상 (약 100평 이상)
    - 중형: 99㎡ ~ 330㎡ (약 30평 ~ 100평)
    - 소형: 99㎡ 미만 (약 30평 미만)
"""

import sys
import time
from typing import Any, Dict, List, Optional
from xml.etree import ElementTree

# 패키지 경로 임포트
from crawler.base_collector import BaseCollector
from crawler.config import CONFIG


class BuildingLedgerCollector(BaseCollector):
    """
    국토교통부 건축물대장 정보 수집기

    건축물대장에서 면적 정보를 조회하여 상가의 대형/중형/소형 분류에 활용합니다.

    Attributes:
        BASE_URL (str): API Base URL
        ES_INDEX (str): Elasticsearch 인덱스명
        PAGE_SIZE (int): 한 페이지당 조회 건수
    """

    BASE_URL = "http://apis.data.go.kr/1613000/BldRgstHubService"
    ES_INDEX = "building_ledger"
    PAGE_SIZE = 100

    # 면적 분류 기준 (단위: ㎡)
    AREA_LARGE_THRESHOLD = 330.0  # 대형 기준 (100평 이상)
    AREA_MEDIUM_THRESHOLD = 99.0  # 중형 기준 (30평 이상)

    # 엔드포인트
    ENDPOINT_TITLE_INFO = "/getBrTitleInfo"  # 총괄표제부 기본정보
    ENDPOINT_EXPOS_AREA = "/getBrExposPubuseAreaInfo"  # 전유공용면적

    def __init__(self):
        """BuildingLedgerCollector 초기화"""
        super().__init__(name="BuildingLedgerCollector")
        self.service_key = CONFIG["data_go_kr"]["service_key"]

        if not self.service_key:
            self.logger.warning(
                "DATA_GO_KR_API_KEY가 설정되지 않았습니다. "
                ".env 파일을 확인하세요."
            )

    def classify_area(self, area: Optional[float]) -> str:
        """
        면적에 따른 상가 분류

        Args:
            area: 면적 (㎡)

        Returns:
            면적 분류 문자열 ('대형', '중형', '소형', '미분류')
        """
        if area is None:
            return "미분류"

        if area >= self.AREA_LARGE_THRESHOLD:
            return "대형"
        elif area >= self.AREA_MEDIUM_THRESHOLD:
            return "중형"
        else:
            return "소형"

    def _parse_bld_mng_no(self, bld_mng_no: str) -> Optional[Dict[str, str]]:
        """
        건물관리번호(bldMngNo)를 파싱하여 API 파라미터 추출

        건물관리번호 구조 (총 25자리):
            - sigunguCd (5자리): 시군구코드
            - bjdongCd (5자리): 법정동코드
            - plcClCd (1자리): 대지구분코드 (0:대지, 1:산)
            - bun (4자리): 번
            - ji (4자리): 지
            - 나머지 (6자리): 기타

        Args:
            bld_mng_no: 건물관리번호 (25자리)

        Returns:
            파라미터 딕셔너리 또는 None
        """
        if not bld_mng_no or len(bld_mng_no) < 19:
            return None

        try:
            return {
                "sigunguCd": bld_mng_no[0:5],
                "bjdongCd": bld_mng_no[5:10],
                "platGbCd": bld_mng_no[10:11],  # 대지구분코드
                "bun": bld_mng_no[11:15],
                "ji": bld_mng_no[15:19],
            }
        except Exception as e:
            self.logger.error(f"건물관리번호 파싱 오류: {bld_mng_no}, {e}")
            return None

    def get_title_info(
        self,
        sigungu_cd: str,
        bjdong_cd: str,
        bun: str,
        ji: str,
        plat_gb_cd: str = "0"
    ) -> Optional[List[Dict[str, Any]]]:
        """
        총괄표제부 기본정보 조회

        Args:
            sigungu_cd: 시군구코드 (5자리)
            bjdong_cd: 법정동코드 (5자리)
            bun: 번 (4자리)
            ji: 지 (4자리)
            plat_gb_cd: 대지구분코드 (0:대지, 1:산)

        Returns:
            건축물대장 총괄표제부 정보 리스트 또는 None
        """
        url = f"{self.BASE_URL}{self.ENDPOINT_TITLE_INFO}"

        params = {
            "serviceKey": self.service_key,
            "sigunguCd": sigungu_cd,
            "bjdongCd": bjdong_cd,
            "platGbCd": plat_gb_cd,
            "bun": bun,
            "ji": ji,
            "numOfRows": self.PAGE_SIZE,
            "pageNo": 1,
        }

        response = self._make_request(url, params=params, max_retries=3)

        if response is None:
            return None

        return self._parse_title_info_response(response.text)

    def _parse_title_info_response(
        self,
        xml_text: str
    ) -> Optional[List[Dict[str, Any]]]:
        """
        총괄표제부 API 응답 파싱

        Args:
            xml_text: XML 응답 텍스트

        Returns:
            파싱된 건축물 정보 리스트 또는 None
        """
        try:
            root = ElementTree.fromstring(xml_text)

            # 에러 체크
            result_code = root.findtext(".//resultCode")
            if result_code != "00":
                result_msg = root.findtext(".//resultMsg")
                self.logger.error(f"API 에러: {result_code} - {result_msg}")
                return None

            items = []
            for item in root.findall(".//item"):
                building = {
                    # 기본 정보
                    "mgmBldrgstPk": item.findtext("mgmBldrgstPk"),  # 관리건축물대장PK
                    "bldNm": item.findtext("bldNm"),  # 건물명
                    "platPlc": item.findtext("platPlc"),  # 대지위치
                    "newPlatPlc": item.findtext("newPlatPlc"),  # 도로명대지위치
                    "mainAtchGbCd": item.findtext("mainAtchGbCd"),  # 주/부속 구분
                    "mainAtchGbCdNm": item.findtext("mainAtchGbCdNm"),  # 주/부속 구분명
                    "regstrGbCd": item.findtext("regstrGbCd"),  # 대장구분코드
                    "regstrGbCdNm": item.findtext("regstrGbCdNm"),  # 대장구분코드명

                    # 면적 정보
                    "totArea": self._parse_float(item.findtext("totArea")),  # 연면적(㎡)
                    "platArea": self._parse_float(item.findtext("platArea")),  # 대지면적(㎡)
                    "archArea": self._parse_float(item.findtext("archArea")),  # 건축면적(㎡)
                    "bcRat": self._parse_float(item.findtext("bcRat")),  # 건폐율(%)
                    "vlRat": self._parse_float(item.findtext("vlRat")),  # 용적률(%)

                    # 건물 정보
                    "mainPurpsCd": item.findtext("mainPurpsCd"),  # 주용도코드
                    "mainPurpsCdNm": item.findtext("mainPurpsCdNm"),  # 주용도명
                    "etcPurps": item.findtext("etcPurps"),  # 기타용도
                    "strctCd": item.findtext("strctCd"),  # 구조코드
                    "strctCdNm": item.findtext("strctCdNm"),  # 구조코드명
                    "grndFlrCnt": self._parse_int(item.findtext("grndFlrCnt")),  # 지상층수
                    "ugrndFlrCnt": self._parse_int(item.findtext("ugrndFlrCnt")),  # 지하층수

                    # 지역/지구 정보
                    "sigunguCd": item.findtext("sigunguCd"),  # 시군구코드
                    "bjdongCd": item.findtext("bjdongCd"),  # 법정동코드
                    "bun": item.findtext("bun"),  # 번
                    "ji": item.findtext("ji"),  # 지

                    # 인허가 정보
                    "pmsDay": item.findtext("pmsDay"),  # 허가일
                    "stcnsDay": item.findtext("stcnsDay"),  # 착공일
                    "useAprDay": item.findtext("useAprDay"),  # 사용승인일

                    # 메타데이터
                    "crtnDay": item.findtext("crtnDay"),  # 생성일자
                }

                # None 값 필터링
                building = {k: v for k, v in building.items() if v is not None}

                items.append(building)

            return items

        except ElementTree.ParseError as e:
            self.logger.error(f"XML 파싱 오류: {e}")
            return None

    def get_expos_pubuse_area(
        self,
        sigungu_cd: str,
        bjdong_cd: str,
        bun: str,
        ji: str,
        plat_gb_cd: str = "0"
    ) -> Optional[List[Dict[str, Any]]]:
        """
        전유공용면적 정보 조회

        상가/점포의 개별 면적을 조회하는 핵심 API입니다.
        총괄표제부의 연면적이 아닌, 개별 호실의 전유면적을 확인합니다.

        Args:
            sigungu_cd: 시군구코드 (5자리)
            bjdong_cd: 법정동코드 (5자리)
            bun: 번 (4자리)
            ji: 지 (4자리)
            plat_gb_cd: 대지구분코드 (0:대지, 1:산)

        Returns:
            전유공용면적 정보 리스트 또는 None
        """
        url = f"{self.BASE_URL}{self.ENDPOINT_EXPOS_AREA}"

        all_items = []
        page_no = 1

        while True:
            params = {
                "serviceKey": self.service_key,
                "sigunguCd": sigungu_cd,
                "bjdongCd": bjdong_cd,
                "platGbCd": plat_gb_cd,
                "bun": bun,
                "ji": ji,
                "numOfRows": self.PAGE_SIZE,
                "pageNo": page_no,
            }

            response = self._make_request(url, params=params, max_retries=3)

            if response is None:
                break

            result = self._parse_expos_area_response(response.text)

            if result is None:
                break

            items, total_count = result

            if not items:
                break

            all_items.extend(items)

            # 다음 페이지 확인
            if len(all_items) >= total_count:
                break

            page_no += 1
            time.sleep(0.05)  # API 부하 방지

        return all_items if all_items else None

    def _parse_expos_area_response(
        self,
        xml_text: str
    ) -> Optional[tuple]:
        """
        전유공용면적 API 응답 파싱

        Args:
            xml_text: XML 응답 텍스트

        Returns:
            (파싱된 면적 정보 리스트, 전체 건수) 튜플 또는 None
        """
        try:
            root = ElementTree.fromstring(xml_text)

            # 에러 체크
            result_code = root.findtext(".//resultCode")
            if result_code != "00":
                result_msg = root.findtext(".//resultMsg")
                self.logger.error(f"API 에러: {result_code} - {result_msg}")
                return None

            total_count = int(root.findtext(".//totalCount") or 0)

            items = []
            for item in root.findall(".//item"):
                area_info = {
                    # 기본 정보
                    "mgmBldrgstPk": item.findtext("mgmBldrgstPk"),  # 관리건축물대장PK
                    "bldNm": item.findtext("bldNm"),  # 건물명
                    "platPlc": item.findtext("platPlc"),  # 대지위치
                    "newPlatPlc": item.findtext("newPlatPlc"),  # 도로명대지위치

                    # 층/호 정보
                    "flrNo": item.findtext("flrNo"),  # 층번호
                    "flrGbCd": item.findtext("flrGbCd"),  # 층구분코드
                    "flrGbCdNm": item.findtext("flrGbCdNm"),  # 층구분명
                    "hoNm": item.findtext("hoNm"),  # 호명칭

                    # 면적 정보 (핵심)
                    "area": self._parse_float(item.findtext("area")),  # 면적(㎡)
                    "exposPubuseGbCd": item.findtext("exposPubuseGbCd"),  # 전유공용구분코드
                    "exposPubuseGbCdNm": item.findtext("exposPubuseGbCdNm"),  # 전유공용구분명
                    "mainAtchGbCd": item.findtext("mainAtchGbCd"),  # 주/부속 구분
                    "mainAtchGbCdNm": item.findtext("mainAtchGbCdNm"),  # 주/부속 구분명

                    # 용도 정보
                    "etcPurps": item.findtext("etcPurps"),  # 기타용도

                    # 지역 코드
                    "sigunguCd": item.findtext("sigunguCd"),  # 시군구코드
                    "bjdongCd": item.findtext("bjdongCd"),  # 법정동코드
                    "bun": item.findtext("bun"),  # 번
                    "ji": item.findtext("ji"),  # 지

                    # 메타데이터
                    "crtnDay": item.findtext("crtnDay"),  # 생성일자
                }

                # None 값 필터링
                area_info = {k: v for k, v in area_info.items() if v is not None}

                items.append(area_info)

            return items, total_count

        except ElementTree.ParseError as e:
            self.logger.error(f"XML 파싱 오류: {e}")
            return None

    def get_building_info_by_bld_mng_no(
        self,
        bld_mng_no: str
    ) -> Optional[Dict[str, Any]]:
        """
        건물관리번호로 건축물대장 정보 조회

        상가 API의 bldMngNo를 사용하여 건축물 면적 정보를 조회합니다.

        Args:
            bld_mng_no: 건물관리번호 (25자리)

        Returns:
            건축물 정보 (면적 분류 포함) 또는 None
        """
        params = self._parse_bld_mng_no(bld_mng_no)
        if params is None:
            return None

        # 총괄표제부 정보 조회
        title_info = self.get_title_info(
            sigungu_cd=params["sigunguCd"],
            bjdong_cd=params["bjdongCd"],
            bun=params["bun"],
            ji=params["ji"],
            plat_gb_cd=params["platGbCd"]
        )

        # 전유공용면적 정보 조회
        expos_area = self.get_expos_pubuse_area(
            sigungu_cd=params["sigunguCd"],
            bjdong_cd=params["bjdongCd"],
            bun=params["bun"],
            ji=params["ji"],
            plat_gb_cd=params["platGbCd"]
        )

        # 결과 병합
        result = {
            "bldMngNo": bld_mng_no,
            "sigunguCd": params["sigunguCd"],
            "bjdongCd": params["bjdongCd"],
            "bun": params["bun"],
            "ji": params["ji"],
            "titleInfo": title_info[0] if title_info else None,
            "exposAreaList": expos_area or [],
        }

        # 면적 계산 및 분류
        tot_area = None
        if title_info and len(title_info) > 0:
            tot_area = title_info[0].get("totArea")

        # 전유면적 합계 계산 (전유 공용 구분이 '전유'인 것만)
        private_area_sum = 0.0
        if expos_area:
            for area in expos_area:
                # 전유공용구분코드가 '1'이 전유
                if area.get("exposPubuseGbCd") == "1":
                    private_area_sum += area.get("area", 0.0)

        result["totArea"] = tot_area
        result["privateAreaSum"] = private_area_sum if private_area_sum > 0 else None
        result["areaClassification"] = self.classify_area(tot_area)

        return result

    def collect_by_bld_mng_nos(
        self,
        bld_mng_nos: List[str],
        delay: float = 0.1
    ) -> List[Dict[str, Any]]:
        """
        건물관리번호 목록으로 건축물대장 정보 일괄 수집

        Args:
            bld_mng_nos: 건물관리번호 목록
            delay: API 호출 간 딜레이 (초)

        Returns:
            수집된 건축물 정보 리스트
        """
        all_items = []

        self.logger.info(
            f"건축물대장 정보 수집 시작: {len(bld_mng_nos)}건"
        )

        for i, bld_mng_no in enumerate(bld_mng_nos):
            if not bld_mng_no:
                continue

            building_info = self.get_building_info_by_bld_mng_no(bld_mng_no)

            if building_info:
                # ES용 메타데이터 추가
                building_info["source"] = "MOLIT"
                building_info["esIndex"] = self.ES_INDEX
                all_items.append(building_info)

            # 진행 상황 로깅
            if (i + 1) % 100 == 0:
                self.logger.info(
                    f"진행: {i + 1}/{len(bld_mng_nos)} "
                    f"(성공: {len(all_items)}건)"
                )

            # API 부하 방지
            time.sleep(delay)

        self.logger.info(
            f"건축물대장 정보 수집 완료: {len(all_items)}건"
        )

        return all_items

    def collect(self) -> List[Dict[str, Any]]:
        """
        기본 수집 메서드

        건축물대장은 건물관리번호(bldMngNo) 기반으로 조회해야 하므로,
        기본 collect()는 빈 리스트를 반환합니다.
        실제 수집은 collect_by_bld_mng_nos()를 사용하세요.

        Returns:
            빈 리스트 (건물관리번호 필요)
        """
        self.logger.warning(
            "건축물대장은 건물관리번호(bldMngNo) 기반으로 수집해야 합니다. "
            "collect_by_bld_mng_nos() 메서드를 사용하세요."
        )
        return []

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

    def run(
        self,
        bld_mng_nos: List[str] = None,
        save_to_es: bool = True
    ) -> Dict[str, int]:
        """
        수집 및 Elasticsearch 저장 실행

        Args:
            bld_mng_nos: 수집할 건물관리번호 목록 (필수)
            save_to_es: Elasticsearch에 저장할지 여부

        Returns:
            저장 결과 통계 (success, failed 카운트)
        """
        self.logger.info("BuildingLedgerCollector 실행 시작")

        if not bld_mng_nos:
            self.logger.warning(
                "건물관리번호 목록이 제공되지 않았습니다. "
                "상가 데이터의 bldMngNo를 사용하세요."
            )
            return {"success": 0, "failed": 0}

        # 데이터 수집
        buildings = self.collect_by_bld_mng_nos(bld_mng_nos)

        if not buildings:
            self.logger.warning("수집된 건축물대장 정보가 없습니다.")
            return {"success": 0, "failed": 0}

        # Elasticsearch 저장
        if save_to_es:
            result = self.save_to_es(
                data=buildings,
                index_name=self.ES_INDEX,
                id_field="bldMngNo"  # 건물관리번호를 문서 ID로 사용
            )

            self.logger.info(
                f"BuildingLedgerCollector 실행 완료: "
                f"성공={result['success']}, 실패={result['failed']}"
            )

            return result

        return {"success": len(buildings), "failed": 0}


# CLI 실행 지원
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="건축물대장 정보 수집기"
    )
    parser.add_argument(
        "--bld-mng-no",
        type=str,
        help="조회할 건물관리번호 (단일 조회 테스트)"
    )
    parser.add_argument(
        "--sigungu-cd",
        type=str,
        help="시군구코드 (5자리)"
    )
    parser.add_argument(
        "--bjdong-cd",
        type=str,
        help="법정동코드 (5자리)"
    )
    parser.add_argument(
        "--bun",
        type=str,
        help="번 (4자리)"
    )
    parser.add_argument(
        "--ji",
        type=str,
        default="0000",
        help="지 (4자리, 기본값: 0000)"
    )

    args = parser.parse_args()

    collector = BuildingLedgerCollector()

    if args.bld_mng_no:
        # 건물관리번호로 조회
        result = collector.get_building_info_by_bld_mng_no(args.bld_mng_no)
        if result:
            print(f"건물관리번호: {result.get('bldMngNo')}")
            print(f"연면적: {result.get('totArea')}㎡")
            print(f"전유면적 합계: {result.get('privateAreaSum')}㎡")
            print(f"면적 분류: {result.get('areaClassification')}")
            if result.get("titleInfo"):
                print(f"건물명: {result['titleInfo'].get('bldNm')}")
                print(f"대지위치: {result['titleInfo'].get('platPlc')}")
        else:
            print("조회 결과 없음")

    elif args.sigungu_cd and args.bjdong_cd and args.bun:
        # 개별 파라미터로 조회
        title_info = collector.get_title_info(
            sigungu_cd=args.sigungu_cd,
            bjdong_cd=args.bjdong_cd,
            bun=args.bun,
            ji=args.ji
        )

        if title_info:
            for info in title_info:
                print(f"건물명: {info.get('bldNm')}")
                print(f"대지위치: {info.get('platPlc')}")
                print(f"연면적: {info.get('totArea')}㎡")
                print(f"면적 분류: {collector.classify_area(info.get('totArea'))}")
                print("-" * 50)
        else:
            print("조회 결과 없음")

    else:
        print("사용법:")
        print("  --bld-mng-no <건물관리번호>")
        print("  또는")
        print("  --sigungu-cd <시군구코드> --bjdong-cd <법정동코드> --bun <번> [--ji <지>]")
