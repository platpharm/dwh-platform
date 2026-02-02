"""
Elasticsearch → Excel 내보내기 스크립트

PharmDB의 Elasticsearch 인덱스 데이터를 Excel 파일로 내보냅니다.
멀티프로세스를 사용하여 각 인덱스를 병렬로 처리합니다.
"""

import argparse
import logging
import os
import sys
from multiprocessing import Pool
from pathlib import Path

import pandas as pd
import urllib3
from dotenv import load_dotenv
from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s: [%(processName)s] %(message)s",
)
logger = logging.getLogger(__name__)

load_dotenv(Path(__file__).resolve().parent.parent / ".env")

ES_HOST = os.getenv("ES_HOST", "localhost")
ES_PORT = int(os.getenv("ES_PORT", "54321"))
ES_USER = os.getenv("ES_USER", "elastic")
ES_PASSWORD = os.getenv("ES_PASSWORD", "")

INDICES = [
    "pharmacy_nic",
    "pharmacy_hira",
    "hospital_nic",
    "hospital_hira",
    "store_semas",
    "medical_type",
    "health_stat",
    "building_ledger",
    "population_stat",
    "business_status",
    "commercial_zone",
    "medical_department",
    "prescription_stat",
]

FILE_NAMES = {
    "pharmacy_nic": "약국_국립중앙의료원",
    "pharmacy_hira": "약국_건강보험심사평가원",
    "hospital_nic": "병원_국립중앙의료원",
    "hospital_hira": "병원_건강보험심사평가원",
    "store_semas": "상가정보_소상공인",
    "medical_type": "의료기관종별",
    "health_stat": "보건의료통계",
    "building_ledger": "건축물대장",
    "population_stat": "인구통계",
    "business_status": "의료기관운영상태",
    "commercial_zone": "상권영역정보",
    "medical_department": "진료과목상세",
    "prescription_stat": "의약품처방통계",
}

COLUMN_TRANSLATIONS = {
    "pharmacy_nic": {
        "hpid": "기관ID",
        "duty_name": "약국명",
        "duty_addr": "주소",
        "duty_tel1": "전화번호",
        "wgs84_lat": "위도",
        "wgs84_lon": "경도",
        "duty_time1s": "월요일_시작",
        "duty_time1c": "월요일_종료",
        "duty_time2s": "화요일_시작",
        "duty_time2c": "화요일_종료",
        "duty_time3s": "수요일_시작",
        "duty_time3c": "수요일_종료",
        "duty_time4s": "목요일_시작",
        "duty_time4c": "목요일_종료",
        "duty_time5s": "금요일_시작",
        "duty_time5c": "금요일_종료",
        "duty_time6s": "토요일_시작",
        "duty_time6c": "토요일_종료",
        "duty_time7s": "일요일_시작",
        "duty_time7c": "일요일_종료",
        "duty_time8s": "공휴일_시작",
        "duty_time8c": "공휴일_종료",
        "post_cdn1": "우편번호1",
        "post_cdn2": "우편번호2",
        "duty_div_nam": "기관구분명",
        "duty_mapimg": "약도이미지",
        "duty_inf": "비고",
        "collected_at": "수집일시",
        "source": "데이터출처",
        "location.lat": "좌표_위도",
        "location.lon": "좌표_경도",
        "location": "좌표",
    },
    "pharmacy_hira": {
        "ykiho": "요양기관코드",
        "yadmNm": "약국명",
        "clCd": "종별코드",
        "clCdNm": "종별코드명",
        "sidoCd": "시도코드",
        "sidoCdNm": "시도명",
        "sgguCd": "시군구코드",
        "sgguCdNm": "시군구명",
        "emdongNm": "읍면동명",
        "addr": "주소",
        "postNo": "우편번호",
        "telno": "전화번호",
        "XPos": "경도",
        "YPos": "위도",
        "estbDd": "개설일자",
    },
    "hospital_nic": {
        "hpid": "기관ID",
        "name": "병원명",
        "address": "주소",
        "tel": "전화번호",
        "latitude": "위도",
        "longitude": "경도",
        "postal_code": "우편번호",
        "duty_div": "기관구분코드",
        "duty_div_name": "기관구분명",
        "duty_eryn": "응급실운영여부",
        "collected_at": "수집일시",
        "source": "데이터출처",
        "location.lat": "좌표_위도",
        "location.lon": "좌표_경도",
    },
    "hospital_hira": {
        "ykiho": "요양기관코드",
        "yadmNm": "병원명",
        "clCd": "종별코드",
        "clCdNm": "종별코드명",
        "sidoCd": "시도코드",
        "sidoCdNm": "시도명",
        "sgguCd": "시군구코드",
        "sgguCdNm": "시군구명",
        "emdongNm": "읍면동명",
        "addr": "주소",
        "postNo": "우편번호",
        "telno": "전화번호",
        "hospUrl": "홈페이지",
        "XPos": "경도",
        "YPos": "위도",
        "estbDd": "개설일자",
        "drTotCnt": "의사총수",
    },
    "store_semas": {
        "bizesId": "상가업소번호",
        "bizesNm": "상호명",
        "brchNm": "지점명",
        "indsLclsCd": "상권업종대분류코드",
        "indsLclsNm": "상권업종대분류명",
        "indsMclsCd": "상권업종중분류코드",
        "indsMclsNm": "상권업종중분류명",
        "indsSclsCd": "상권업종소분류코드",
        "indsSclsNm": "상권업종소분류명",
        "ksicCd": "표준산업분류코드",
        "ksicNm": "표준산업분류명",
        "ctprvnCd": "시도코드",
        "ctprvnNm": "시도명",
        "signguCd": "시군구코드",
        "signguNm": "시군구명",
        "adongCd": "행정동코드",
        "adongNm": "행정동명",
        "ldongCd": "법정동코드",
        "ldongNm": "법정동명",
        "lnoAdr": "지번주소",
        "rdnmAdr": "도로명주소",
        "bldNm": "건물명",
        "bldMngNo": "건물관리번호",
        "flrNo": "층정보",
        "lon": "경도",
        "lat": "위도",
        "source": "데이터출처",
        "esIndex": "ES인덱스",
    },
    "medical_type": {
        "ykiho": "요양기관코드",
        "yadmNm": "기관명",
        "clCd": "종별코드",
        "clCdNm": "종별코드명",
        "sidoCd": "시도코드",
        "sidoCdNm": "시도명",
        "sgguCd": "시군구코드",
        "sgguCdNm": "시군구명",
        "emdongNm": "읍면동명",
        "addr": "주소",
        "postNo": "우편번호",
        "telno": "전화번호",
        "hospUrl": "홈페이지",
        "XPos": "경도",
        "YPos": "위도",
        "estbDd": "개설일자",
        "drTotCnt": "의사총수",
        "pnursCnt": "간호사수",
        "collected_at": "수집일시",
    },
    "health_stat": {
        "stat_type": "통계유형",
        "st_yy": "기준연도",
        "sido_cd": "시도코드",
        "sido_cd_nm": "시도명",
        "sggu_cd": "시군구코드",
        "sggu_cd_nm": "시군구명",
        "pat_cnt": "환자수",
        "rcpt_cnt": "청구건수",
        "mdc_amt_sum": "진료비총액",
        "mdc_amt_avg": "진료비평균",
        "ins_amt_sum": "보험자부담총액",
        "ins_amt_avg": "보험자부담평균",
        "own_amt_sum": "본인부담총액",
        "own_amt_avg": "본인부담평균",
        "prsc_cnt": "처방건수",
        "prsc_amt": "처방금액",
        "drug_cnt": "약품수",
        "drug_use_cnt": "약품사용건수",
        "collected_at": "수집일시",
    },
    "population_stat": {
        "srch_ym": "조사년월",
        "stdg_cd": "행정구역코드",
        "stdg_nm": "행정구역명",
        "tot_nmpr_cnt": "총인구수",
        "male_nmpr_cnt": "남성인구수",
        "feml_nmpr_cnt": "여성인구수",
        "hh_cnt": "세대수",
        "collected_at": "수집일시",
    },
    "business_status": {
        "ykiho": "요양기관코드",
        "yadmNm": "기관명",
        "clCd": "종별코드",
        "clCdNm": "종별코드명",
        "sidoCd": "시도코드",
        "sidoCdNm": "시도명",
        "sgguCd": "시군구코드",
        "sgguCdNm": "시군구명",
        "emdongNm": "읍면동명",
        "addr": "주소",
        "telno": "전화번호",
        "estbDd": "개설일자",
        "XPos": "경도",
        "YPos": "위도",
        "drTotCnt": "의사총수",
        "collected_at": "수집일시",
    },
    "commercial_zone": {
        "trarNo": "상권번호",
        "trarNm": "상권명",
        "trarTypeCd": "상권유형코드",
        "trarTypeNm": "상권유형명",
        "ctprvnCd": "시도코드",
        "ctprvnNm": "시도명",
        "signguCd": "시군구코드",
        "signguNm": "시군구명",
        "adongCd": "행정동코드",
        "adongNm": "행정동명",
        "centerLon": "중심경도",
        "centerLat": "중심위도",
        "areaSize": "면적",
        "coords": "좌표",
        "collected_at": "수집일시",
    },
    "medical_department": {
        "ykiho": "요양기관코드",
        "yadmNm": "기관명",
        "clCd": "종별코드",
        "clCdNm": "종별코드명",
        "sidoCd": "시도코드",
        "sidoCdNm": "시도명",
        "sgguCd": "시군구코드",
        "sgguCdNm": "시군구명",
        "addr": "주소",
        "dgsbjtCd": "진료과목코드",
        "dgsbjtCdNm": "진료과목명",
        "drTotCnt": "의사총수",
        "mdeptSdrCnt": "전문의수",
        "mdeptGdrCnt": "일반의수",
        "mdeptIntnCnt": "인턴수",
        "mdeptResdntCnt": "레지던트수",
        "XPos": "경도",
        "YPos": "위도",
        "collected_at": "수집일시",
    },
    "prescription_stat": {
        "stat_type": "통계유형",
        "diag_ym": "진료년월",
        "sido_nm": "시도명",
        "sggu_nm": "시군구명",
        "mdcin_nm": "의약품명",
        "mdcin_cd": "의약품코드",
        "prscrpt_cnt": "처방건수",
        "prscrpt_amt": "처방금액",
        "tot_use_qty": "총사용량",
        "collected_at": "수집일시",
    },
    "building_ledger": {
        "mgmBldrgstPk": "건축물대장PK",
        "bldNm": "건물명",
        "platPlc": "대지위치",
        "newPlatPlc": "도로명대지위치",
        "mainAtchGbCd": "주부속구분코드",
        "mainAtchGbCdNm": "주부속구분코드명",
        "regstrGbCd": "대장구분코드",
        "regstrGbCdNm": "대장구분코드명",
        "totArea": "연면적",
        "platArea": "대지면적",
        "archArea": "건축면적",
        "bcRat": "건폐율",
        "vlRat": "용적률",
        "mainPurpsCd": "주용도코드",
        "mainPurpsCdNm": "주용도코드명",
        "strctCd": "구조코드",
        "strctCdNm": "구조코드명",
        "grndFlrCnt": "지상층수",
        "ugrndFlrCnt": "지하층수",
        "sigunguCd": "시군구코드",
        "bjdongCd": "법정동코드",
        "pmsDay": "허가일",
        "useAprDay": "사용승인일",
        "crtnDay": "생성일",
    },
}


def create_es_client() -> Elasticsearch:
    kwargs = {
        "hosts": [{"host": ES_HOST, "port": ES_PORT, "scheme": "https"}],
        "verify_certs": False,
        "ssl_show_warn": False,
        "request_timeout": 60,
        "max_retries": 3,
        "retry_on_timeout": True,
    }
    if ES_USER and ES_PASSWORD:
        kwargs["basic_auth"] = (ES_USER, ES_PASSWORD)
    return Elasticsearch(**kwargs)


def export_single_index(args: tuple) -> dict:
    """
    단일 인덱스를 Excel로 내보내기 (멀티프로세스 워커 함수)

    각 프로세스에서 독립적인 ES 클라이언트를 생성하여 처리합니다.
    """
    index_name, output_dir_str = args
    output_dir = Path(output_dir_str)
    result = {"index": index_name, "success": False, "count": 0, "error": None}

    try:
        es = create_es_client()

        if not es.indices.exists(index=index_name):
            result["error"] = "인덱스가 존재하지 않음"
            logger.warning(f"[{index_name}] 인덱스가 존재하지 않습니다. 건너뜁니다.")
            es.close()
            return result

        count = es.count(index=index_name)["count"]
        logger.info(f"[{index_name}] {count}건 문서 조회 중...")

        if count == 0:
            result["error"] = "문서 없음"
            logger.warning(f"[{index_name}] 문서가 없습니다. 건너뜁니다.")
            es.close()
            return result

        docs = []
        for hit in scan(es, index=index_name, query={"query": {"match_all": {}}}, size=1000):
            docs.append(hit["_source"])

        es.close()

        df = pd.json_normalize(docs)

        translations = COLUMN_TRANSLATIONS.get(index_name, {})
        if translations:
            df.rename(columns=translations, inplace=True)

        file_label = FILE_NAMES.get(index_name, index_name)
        filepath = output_dir / f"{file_label}.xlsx"
        df.to_excel(filepath, index=False, engine="openpyxl")

        size_mb = filepath.stat().st_size / 1024 / 1024
        logger.info(f"[{index_name}] {len(docs)}건 → {filepath.name} ({size_mb:.1f} MB)")

        result["success"] = True
        result["count"] = len(docs)

    except Exception as e:
        result["error"] = str(e)
        logger.error(f"[{index_name}] 내보내기 실패: {e}")

    return result


def main():
    parser = argparse.ArgumentParser(description="PharmDB Elasticsearch → Excel 내보내기")
    parser.add_argument(
        "--indices",
        nargs="+",
        choices=INDICES,
        default=INDICES,
        help="내보낼 인덱스 목록 (기본: 전체)",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default=str(Path(__file__).resolve().parent.parent / "excel"),
        help="출력 디렉터리 (기본: pharm-db/excel/)",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=4,
        help="병렬 프로세스 수 (기본: 4)",
    )
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    logger.info(f"출력 디렉터리: {output_dir}")
    logger.info(f"병렬 프로세스 수: {args.workers}")

    es = create_es_client()
    if not es.ping():
        logger.error("Elasticsearch 연결 실패. SSH 터널 및 .env 설정을 확인하세요.")
        sys.exit(1)
    es.close()
    logger.info("Elasticsearch 연결 확인 완료")

    task_args = [(index_name, str(output_dir)) for index_name in args.indices]

    with Pool(processes=args.workers) as pool:
        results = pool.map(export_single_index, task_args)

    success_count = sum(1 for r in results if r["success"])
    fail_count = sum(1 for r in results if not r["success"])
    total_docs = sum(r["count"] for r in results)

    logger.info(f"완료 - 성공: {success_count}, 실패/건너뜀: {fail_count}, 총 문서: {total_docs}건")
    logger.info(f"Excel 파일 위치: {output_dir}")


if __name__ == "__main__":
    main()
