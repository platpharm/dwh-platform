"""
Kibana Data View 생성 스크립트

PharmDB의 9개 Elasticsearch 인덱스에 대한 Kibana Data View를 생성합니다.
"""

import argparse
import json
import logging
import os
import sys
from typing import Any, Optional

import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

KIBANA_HOST = os.getenv("KIBANA_HOST", "localhost")
KIBANA_PORT = int(os.getenv("KIBANA_PORT", "5601"))
KIBANA_SCHEME = os.getenv("KIBANA_SCHEME", "https")
KIBANA_USER = os.getenv("KIBANA_USER", "elastic")
KIBANA_PASSWORD = os.getenv("KIBANA_PASSWORD", "")
KIBANA_SPACE = os.getenv("KIBANA_SPACE", "default")

DATA_VIEWS = [
    {
        "name": "PharmDB - 약국 (국립중앙의료원)",
        "title": "pharmacy_nic",
        "timeFieldName": "collected_at",
    },
    {
        "name": "PharmDB - 약국 (건강보험심사평가원)",
        "title": "pharmacy_hira",
        "timeFieldName": None,
    },
    {
        "name": "PharmDB - 병원 (국립중앙의료원)",
        "title": "hospital_nic",
        "timeFieldName": "collected_at",
    },
    {
        "name": "PharmDB - 병원 (건강보험심사평가원)",
        "title": "hospital_hira",
        "timeFieldName": None,
    },
    {
        "name": "PharmDB - 상가정보 (소상공인)",
        "title": "store_semas",
        "timeFieldName": None,
    },
    {
        "name": "PharmDB - 의료기관종별",
        "title": "medical_type",
        "timeFieldName": "collected_at",
    },
    {
        "name": "PharmDB - 보건의료 통계",
        "title": "health_stat",
        "timeFieldName": "collected_at",
    },
    {
        "name": "PharmDB - 플랫팜 발주",
        "title": "order_platpharm",
        "timeFieldName": None,
    },
    {
        "name": "PharmDB - 건축물대장",
        "title": "building_ledger",
        "timeFieldName": None,
    },
]


def get_kibana_url() -> str:
    base = f"{KIBANA_SCHEME}://{KIBANA_HOST}:{KIBANA_PORT}"
    if KIBANA_SPACE and KIBANA_SPACE != "default":
        return f"{base}/s/{KIBANA_SPACE}"
    return base


def create_data_view(
    session: requests.Session,
    name: str,
    title: str,
    time_field: Optional[str],
    overwrite: bool = False,
) -> dict[str, Any]:
    base_url = get_kibana_url()
    url = f"{base_url}/api/data_views/data_view"

    data_view_spec: dict[str, Any] = {
        "title": title,
        "name": name,
    }
    if time_field:
        data_view_spec["timeFieldName"] = time_field

    payload: dict[str, Any] = {"data_view": data_view_spec}
    if overwrite:
        payload["override"] = True

    response = session.post(url, json=payload)

    if response.status_code == 200:
        result = response.json()
        dv_id = result.get("data_view", {}).get("id", "unknown")
        logger.info(f"[OK] {name} (index: {title}, id: {dv_id})")
        return result
    elif response.status_code == 409:
        logger.warning(f"[SKIP] {name} - 이미 존재합니다 (--overwrite 옵션으로 덮어쓰기 가능)")
        return {"skipped": True}
    else:
        logger.error(f"[FAIL] {name} - HTTP {response.status_code}: {response.text}")
        return {"error": response.text}


def list_existing_data_views(session: requests.Session) -> list[dict]:
    base_url = get_kibana_url()
    url = f"{base_url}/api/data_views"
    response = session.get(url)
    if response.status_code == 200:
        return response.json().get("data_view", [])
    return []


def delete_data_view(session: requests.Session, data_view_id: str) -> bool:
    base_url = get_kibana_url()
    url = f"{base_url}/api/data_views/data_view/{data_view_id}"
    response = session.delete(url)
    return response.status_code == 200


def build_session() -> requests.Session:
    session = requests.Session()
    session.verify = False
    session.headers.update({
        "kbn-xsrf": "true",
        "Content-Type": "application/json",
    })
    if KIBANA_USER and KIBANA_PASSWORD:
        session.auth = (KIBANA_USER, KIBANA_PASSWORD)
    return session


def main():
    parser = argparse.ArgumentParser(description="PharmDB Kibana Data View 생성")
    parser.add_argument("--overwrite", action="store_true", help="기존 Data View 덮어쓰기")
    parser.add_argument("--list", action="store_true", help="기존 Data View 목록 조회")
    parser.add_argument("--dry-run", action="store_true", help="실제 생성 없이 설정 확인")
    parser.add_argument(
        "--indices",
        nargs="+",
        help="특정 인덱스만 생성 (예: pharmacy_nic hospital_nic)",
    )
    args = parser.parse_args()

    session = build_session()

    if args.list:
        existing = list_existing_data_views(session)
        if not existing:
            logger.info("등록된 Data View가 없습니다.")
        else:
            for dv in existing:
                logger.info(f"  {dv.get('name', 'N/A')} -> {dv.get('title', 'N/A')} (id: {dv.get('id')})")
        return

    targets = DATA_VIEWS
    if args.indices:
        targets = [dv for dv in DATA_VIEWS if dv["title"] in args.indices]
        if not targets:
            logger.error(f"지정한 인덱스를 찾을 수 없습니다: {args.indices}")
            sys.exit(1)

    logger.info(f"Kibana: {get_kibana_url()}")
    logger.info(f"생성 대상: {len(targets)}개 Data View\n")

    if args.dry_run:
        for dv in targets:
            time_field = dv["timeFieldName"] or "(없음)"
            logger.info(f"  {dv['name']} | index: {dv['title']} | time_field: {time_field}")
        logger.info("\n--dry-run 모드: 실제 생성하지 않았습니다.")
        return

    success, skip, fail = 0, 0, 0
    for dv in targets:
        result = create_data_view(
            session,
            name=dv["name"],
            title=dv["title"],
            time_field=dv["timeFieldName"],
            overwrite=args.overwrite,
        )
        if result.get("skipped"):
            skip += 1
        elif result.get("error"):
            fail += 1
        else:
            success += 1

    logger.info(f"\n완료 - 성공: {success}, 스킵: {skip}, 실패: {fail}")


if __name__ == "__main__":
    main()
