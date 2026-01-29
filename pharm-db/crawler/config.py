"""
Crawler Configuration Module

크롤러 모듈의 설정을 관리합니다.
환경 변수에서 API 키와 접속 정보를 로드합니다.
"""

import os
from dotenv import load_dotenv

load_dotenv()


class Config:
    """크롤러 설정 클래스"""

    # 공공데이터 포털 (data.go.kr) API
    DATA_GO_KR_API_KEY = os.getenv("DATA_GO_KR_API_KEY", "")

    # Elasticsearch 설정
    ES_HOST = os.getenv("ES_HOST", "localhost")
    ES_PORT = int(os.getenv("ES_PORT", "54321"))
    ES_USER = os.getenv("ES_USER", "elastic")
    ES_PASSWORD = os.getenv("ES_PASSWORD", "")

    # RDBMS (PostgreSQL/MySQL) - 플랫팜 발주 데이터
    RDBMS_HOST = os.getenv("RDBMS_HOST", "localhost")
    RDBMS_PORT = int(os.getenv("RDBMS_PORT", "12345"))
    RDBMS_DATABASE = os.getenv("RDBMS_DATABASE", "")
    RDBMS_USER = os.getenv("RDBMS_USER", "")
    RDBMS_PASSWORD = os.getenv("RDBMS_PASSWORD", "")

    # HTTP 요청 설정
    MAX_RETRIES = 3
    RETRY_DELAY = 1.0
    DEFAULT_TIMEOUT = 30
    DEFAULT_PAGE_SIZE = 100

    # API 엔드포인트
    ENDPOINTS = {
        # 국립중앙의료원 약국 정보
        "pharmacy_nic": "http://apis.data.go.kr/B552657/ErmctInsttInfoInqireService/getParmacyFullDown",
        # 건강보험심사평가원 약국 정보
        "pharmacy_hira": "http://apis.data.go.kr/B551182/pharmacyInfoService/getParmacyBasisList",
        # 국립중앙의료원 병원 정보
        "hospital_nic": "http://apis.data.go.kr/B552657/HsptlAsembySearchService/getHsptlMdcncListInfoInqire",
        # 건강보험심사평가원 병원 정보
        "hospital_hira": "http://apis.data.go.kr/B551182/hospInfoServicev2/getHospBasisList",
        # 소상공인진흥공단 상가 정보
        "store_semas": "http://apis.data.go.kr/B553077/api/open/sdsc2/storeListInDong",
        # 건강보험심사평가원 의료기관종별
        "medical_type": "http://apis.data.go.kr/B551182/hospInfoServicev2/getHospBasisList",
        # 건강보험심사평가원 보건의료 통계
        "health_stat_medical_cost": "https://www.data.go.kr/cmm/cmm/fileDownload.do?atchFileId=FILE_000000003179263&fileDetailSn=1&insertDataPrcus=N",
        "health_stat_medical_type": "https://www.data.go.kr/cmm/cmm/fileDownload.do?atchFileId=FILE_000000003547916&fileDetailSn=1&insertDataPrcus=N",
        "health_stat_drug_usage": "https://apis.data.go.kr/B551182/msupUserInfoService1.2/getMeftDivAreaList1.2",
        # 국토교통부 건축물대장
        "building_ledger": "http://apis.data.go.kr/1613000/BldRgstHubService/getBrTitleInfo",
    }


config = Config()


CONFIG = {
    "elasticsearch": {
        "host": config.ES_HOST,
        "port": config.ES_PORT,
        "user": config.ES_USER,
        "password": config.ES_PASSWORD,
    },
    "data_go_kr": {
        "service_key": config.DATA_GO_KR_API_KEY,
    },
    "rdbms": {
        "host": config.RDBMS_HOST,
        "port": config.RDBMS_PORT,
        "database": config.RDBMS_DATABASE,
        "user": config.RDBMS_USER,
        "password": config.RDBMS_PASSWORD,
    },
    "endpoints": config.ENDPOINTS,
}
