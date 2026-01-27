#!/usr/bin/env python3
"""
ES 인덱스 정리 스크립트
잘못된 데이터가 저장된 인덱스들을 전체 삭제합니다.

사용법:
    python scripts/cleanup_es_indices.py
    python scripts/cleanup_es_indices.py --dry-run  # 실제 삭제하지 않고 확인만
"""
import argparse
import logging
import sys
from pathlib import Path

# 프로젝트 루트를 path에 추가
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from shared.clients.es_client import es_client
from shared.config import ESIndex

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# 삭제 대상 인덱스 목록 (파이프라인 결과 인덱스들)
INDICES_TO_CLEANUP = [
    ("RANKING_RESULT", ESIndex.RANKING_RESULT),
    ("TARGETING_RESULT", ESIndex.TARGETING_RESULT),
    ("CLUSTERING_RESULT", ESIndex.CLUSTERING_RESULT),
    ("PREPROCESSED_PHARMACY", ESIndex.PREPROCESSED_PHARMACY),
    ("PREPROCESSED_ORDER", ESIndex.PREPROCESSED_ORDER),
    ("TREND_PRODUCT_MAPPING", ESIndex.TREND_PRODUCT_MAPPING),
    ("FORECASTING_RESULT", ESIndex.FORECASTING_RESULT),
]


def get_index_count(index_name: str) -> int:
    """인덱스의 문서 수 조회"""
    try:
        result = es_client.client.count(index=index_name)
        return result.get('count', 0)
    except Exception:
        return 0


def cleanup_index(index_name: str, dry_run: bool = False) -> dict:
    """단일 인덱스의 모든 문서 삭제"""
    try:
        # 삭제 전 문서 수 확인
        before_count = get_index_count(index_name)

        if before_count == 0:
            return {
                "success": True,
                "index": index_name,
                "deleted": 0,
                "message": "인덱스가 비어있음"
            }

        if dry_run:
            return {
                "success": True,
                "index": index_name,
                "deleted": 0,
                "message": f"[DRY-RUN] {before_count}개 문서 삭제 예정"
            }

        # 모든 문서 삭제
        result = es_client.delete_by_query(
            index=index_name,
            query={"match_all": {}}
        )

        deleted_count = result.get('deleted', 0)

        return {
            "success": True,
            "index": index_name,
            "deleted": deleted_count,
            "message": f"{deleted_count}개 문서 삭제 완료"
        }

    except Exception as e:
        return {
            "success": False,
            "index": index_name,
            "deleted": 0,
            "message": f"삭제 실패: {str(e)}"
        }


def cleanup_all_indices(dry_run: bool = False) -> bool:
    """모든 대상 인덱스 정리"""
    logger.info("=" * 60)
    logger.info("MediTrend ES 인덱스 정리 시작")
    logger.info("=" * 60)

    # ES 연결 확인
    if not es_client.health_check():
        logger.error("Elasticsearch 연결 실패!")
        return False

    logger.info("Elasticsearch 연결 성공")

    if dry_run:
        logger.info("[DRY-RUN 모드] 실제 삭제하지 않고 확인만 합니다.")

    logger.info("-" * 60)

    total_deleted = 0
    failed_count = 0

    for alias, index_name in INDICES_TO_CLEANUP:
        result = cleanup_index(index_name, dry_run)

        if result["success"]:
            total_deleted += result["deleted"]
            logger.info(f"✓ {alias:25} ({index_name})")
            logger.info(f"  └─ {result['message']}")
        else:
            failed_count += 1
            logger.error(f"✗ {alias:25} ({index_name})")
            logger.error(f"  └─ {result['message']}")

    logger.info("-" * 60)
    logger.info(f"총 {total_deleted}개 문서 삭제됨")

    if failed_count > 0:
        logger.warning(f"{failed_count}개 인덱스 처리 실패")
        return False

    logger.info("=" * 60)
    logger.info("ES 인덱스 정리 완료")
    logger.info("=" * 60)

    return True


def main():
    parser = argparse.ArgumentParser(
        description="MediTrend ES 인덱스 정리 스크립트"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="실제 삭제하지 않고 확인만 합니다"
    )

    args = parser.parse_args()

    success = cleanup_all_indices(dry_run=args.dry_run)

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
