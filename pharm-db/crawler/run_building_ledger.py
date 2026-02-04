"""
건축물대장 실제 데이터 수집 스크립트

store_semas의 bldMngNo를 사용하여 건축물대장 정보를 수집합니다.
"""

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

from crawler.collectors.building_ledger import BuildingLedgerCollector
from crawler.config import CONFIG


def get_es_client():
    es_config = CONFIG["elasticsearch"]
    return Elasticsearch(
        hosts=[f"https://{es_config['host']}:{es_config['port']}"],
        basic_auth=(es_config['user'], es_config['password']),
        verify_certs=False,
        ssl_show_warn=False,
    )


def get_unique_bld_mng_nos(es_client, limit: int = 1000) -> list:
    query = {
        "query": {
            "bool": {
                "must": [
                    {"exists": {"field": "bldMngNo"}}
                ]
            }
        },
        "size": 0,
        "aggs": {
            "unique_bld_mng_no": {
                "terms": {
                    "field": "bldMngNo.keyword",
                    "size": limit
                }
            }
        }
    }

    response = es_client.search(index="store_semas", body=query)

    buckets = response.get("aggregations", {}).get("unique_bld_mng_no", {}).get("buckets", [])

    return [bucket["key"] for bucket in buckets if bucket["key"]]


def main():
    parser = argparse.ArgumentParser(description="건축물대장 실제 데이터 수집")
    parser.add_argument("--limit", type=int, default=1000, help="수집할 건물 수")
    parser.add_argument("--delay", type=float, default=0.05, help="API 호출 간 딜레이(초)")
    parser.add_argument("--dry-run", action="store_true", help="수집만 하고 ES 저장 안함")

    args = parser.parse_args()

    print(f"[{datetime.now()}] 건축물대장 수집 시작")
    print(f"  - 수집 대상: {args.limit}건")
    print(f"  - API 딜레이: {args.delay}초")

    es_client = get_es_client()

    print(f"\n[{datetime.now()}] store_semas에서 건물관리번호 추출 중...")
    bld_mng_nos = get_unique_bld_mng_nos(es_client, args.limit)
    print(f"  - 추출된 건물관리번호: {len(bld_mng_nos)}건")

    if not bld_mng_nos:
        print("건물관리번호가 없습니다.")
        sys.exit(1)

    print(f"\n[{datetime.now()}] 건축물대장 API 수집 시작...")
    collector = BuildingLedgerCollector()
    buildings = collector.collect_by_bld_mng_nos(bld_mng_nos, delay=args.delay)

    print(f"\n[{datetime.now()}] 수집 완료: {len(buildings)}건")

    if not buildings:
        print("수집된 건축물대장 정보가 없습니다.")
        sys.exit(1)

    for b in buildings:
        b["collected_at"] = datetime.now().isoformat()
        title_info = b.pop("titleInfo", None)
        if title_info:
            for key, value in title_info.items():
                b[key] = value
        b.pop("exposAreaList", None)

    json_path = Path(__file__).resolve().parent.parent / "data" / "building_ledger.json"
    json_path.parent.mkdir(parents=True, exist_ok=True)

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(buildings, f, ensure_ascii=False, indent=2)
    print(f"\n[{datetime.now()}] JSON 저장 완료: {json_path}")

    if args.dry_run:
        print("\n[Dry Run] ES 저장 건너뜀")
        print("\n샘플 데이터 (처음 3건):")
        for i, b in enumerate(buildings[:3]):
            print(f"\n--- 건물 {i+1} ---")
            print(f"  건물관리번호: {b.get('bldMngNo')}")
            title_info = b.get('titleInfo', {})
            if title_info:
                print(f"  건물명: {title_info.get('bldNm')}")
                print(f"  대지위치: {title_info.get('platPlc')}")
                print(f"  연면적: {title_info.get('totArea')}㎡")
                print(f"  주용도: {title_info.get('mainPurpsCdNm')}")
            print(f"  면적분류: {b.get('areaClassification')}")
    else:
        print(f"\n[{datetime.now()}] Elasticsearch 저장 중...")
        success_count = 0
        failed_count = 0

        def generate_actions():
            for doc in buildings:
                yield {
                    "_index": collector.ES_INDEX,
                    "_id": doc.get("bldMngNo"),
                    "_source": doc,
                }

        try:
            success, errors = bulk(
                es_client,
                generate_actions(),
                chunk_size=50,
                request_timeout=120,
                raise_on_error=False,
            )
            success_count = success
            if errors:
                failed_count = len(errors)
        except Exception as e:
            print(f"  - ES 저장 오류: {e}")
            failed_count = len(buildings)

        print(f"  - 성공: {success_count}건")
        print(f"  - 실패: {failed_count}건")

    print(f"\n[{datetime.now()}] 완료!")

    return buildings


if __name__ == "__main__":
    main()
