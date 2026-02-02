"""
Crawler Main Orchestrator

모든 수집기를 병렬로 실행하고 결과를 수집하는 오케스트레이터입니다.

사용법:
    # 전체 수집기 실행
    python main.py

    # 특정 수집기만 실행
    python main.py --collectors pharmacy_nic pharmacy_hira

    # Dry-run 모드 (실제 실행 없이 설정 확인)
    python main.py --dry-run

    # 상세 로그 출력
    python main.py --verbose
"""

import argparse
import importlib
import logging
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

_current_dir = Path(__file__).resolve().parent
_project_root = _current_dir.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))
if str(_current_dir) not in sys.path:
    sys.path.insert(0, str(_current_dir))

COLLECTOR_REGISTRY: Dict[str, str] = {
    "pharmacy_nic": "collectors.pharmacy_nic.PharmacyNICCollector",
    "pharmacy_hira": "collectors.pharmacy_hira.PharmacyHIRACollector",
    "hospital_nic": "collectors.hospital_nic.HospitalNICCollector",
    "hospital_hira": "collectors.hospital_hira.HospitalHIRACollector",
    "store_semas": "collectors.store_semas.StoreSEMASCollector",
    "medical_type": "collectors.medical_type.MedicalTypeCollector",
    "health_stat": "collectors.health_stat.HealthStatCollector",

    "building_ledger": "collectors.building_ledger.BuildingLedgerCollector",
    "population_stat": "collectors.population_stat.PopulationStatCollector",
    "business_status": "collectors.business_status.BusinessStatusCollector",
    "commercial_zone": "collectors.commercial_zone.CommercialZoneCollector",
    "medical_department": "collectors.medical_department.MedicalDepartmentCollector",
    "prescription_stat": "collectors.prescription_stat.PrescriptionStatCollector",
}

DEFAULT_COLLECTORS = list(COLLECTOR_REGISTRY.keys())
MAX_WORKERS = 10


@dataclass
class CollectorResult:
    """수집기 실행 결과를 저장하는 데이터 클래스"""

    name: str
    success: bool = False
    count: int = 0
    elapsed_seconds: float = 0.0
    error: Optional[str] = None
    started_at: Optional[str] = None
    finished_at: Optional[str] = None
    details: Dict[str, Any] = field(default_factory=dict)


def setup_logging(verbose: bool = False) -> logging.Logger:
    """
    로깅 설정

    Args:
        verbose: True면 DEBUG 레벨, False면 INFO 레벨

    Returns:
        설정된 로거
    """
    level = logging.DEBUG if verbose else logging.INFO

    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[
            logging.StreamHandler(sys.stdout),
        ],
    )

    return logging.getLogger("CrawlerOrchestrator")


def import_collector_class(collector_name: str) -> Optional[type]:
    """
    수집기 클래스를 동적으로 임포트

    Args:
        collector_name: 수집기 이름 (예: pharmacy_nic)

    Returns:
        수집기 클래스 또는 None (임포트 실패 시)
    """
    if collector_name not in COLLECTOR_REGISTRY:
        return None

    module_path = COLLECTOR_REGISTRY[collector_name]
    module_name, class_name = module_path.rsplit(".", 1)

    logger = logging.getLogger("CrawlerOrchestrator")

    import_paths = [
        f"crawler.{module_name}",
        module_name,
    ]

    last_error = None
    for import_path in import_paths:
        try:
            module = importlib.import_module(import_path)
            return getattr(module, class_name)
        except (ImportError, AttributeError) as e:
            last_error = e
            continue

    logger.error(f"수집기 '{collector_name}' 임포트 실패: {last_error}")
    return None


def run_collector(collector_name: str, dry_run: bool = False) -> CollectorResult:
    """
    단일 수집기 실행

    Args:
        collector_name: 수집기 이름
        dry_run: True면 실제 실행 없이 설정만 확인

    Returns:
        수집기 실행 결과
    """
    logger = logging.getLogger(f"Collector.{collector_name}")
    result = CollectorResult(name=collector_name)
    result.started_at = datetime.now().isoformat()

    start_time = time.time()

    try:
        collector_class = import_collector_class(collector_name)

        if collector_class is None:
            result.error = f"수집기 클래스를 찾을 수 없습니다: {collector_name}"
            logger.error(result.error)
            return result

        logger.info(f"수집기 시작: {collector_name}")

        if dry_run:
            # Dry-run 모드: 클래스 인스턴스화만 확인
            logger.info(f"[DRY-RUN] {collector_name} - 실제 실행 건너뜀")
            collector = collector_class()
            result.success = True
            result.details = {
                "mode": "dry_run",
                "class": collector_class.__name__,
            }
        else:
            collector = collector_class()

            # run() 메서드가 있으면 사용, 없으면 collect() 사용
            if hasattr(collector, "run") and callable(collector.run):
                run_result = collector.run()
            elif hasattr(collector, "collect") and callable(collector.collect):
                data = collector.collect()
                run_result = {
                    "total_collected": len(data) if isinstance(data, list) else 0
                }
            else:
                raise AttributeError(
                    f"수집기에 run() 또는 collect() 메서드가 없습니다."
                )

            result.success = True

            if isinstance(run_result, dict):
                result.count = run_result.get(
                    "total_collected",
                    run_result.get("success", 0)
                )
                result.details = run_result
            elif isinstance(run_result, (list, tuple)):
                result.count = len(run_result)
            elif isinstance(run_result, int):
                result.count = run_result

            logger.info(f"수집기 완료: {collector_name} - {result.count}건 수집")

    except Exception as e:
        result.success = False
        result.error = str(e)
        logger.exception(f"수집기 실행 중 오류 발생: {collector_name}")

    finally:
        result.elapsed_seconds = round(time.time() - start_time, 2)
        result.finished_at = datetime.now().isoformat()

    return result


def run_parallel(
    collectors: List[str],
    max_workers: int = MAX_WORKERS,
    dry_run: bool = False,
) -> List[CollectorResult]:
    """
    수집기들을 병렬로 실행

    Args:
        collectors: 실행할 수집기 이름 목록
        max_workers: 최대 병렬 워커 수
        dry_run: Dry-run 모드 여부

    Returns:
        수집기 실행 결과 목록
    """
    logger = logging.getLogger("CrawlerOrchestrator")
    results: List[CollectorResult] = []

    logger.info("=" * 60)
    logger.info("병렬 수집 시작")
    logger.info(f"대상 수집기: {len(collectors)}개")
    logger.info(f"병렬 워커 수: {max_workers}")
    logger.info(f"Dry-run 모드: {dry_run}")
    logger.info("=" * 60)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_collector = {
            executor.submit(run_collector, name, dry_run): name
            for name in collectors
        }

        for future in as_completed(future_to_collector):
            collector_name = future_to_collector[future]
            try:
                result = future.result()
                results.append(result)
            except Exception as e:
                logger.error(f"수집기 '{collector_name}' 실행 실패: {e}")
                results.append(CollectorResult(
                    name=collector_name,
                    success=False,
                    error=str(e),
                ))

    return results


def print_summary(results: List[CollectorResult], total_elapsed: float) -> None:
    """
    실행 결과 요약 출력

    Args:
        results: 수집기 실행 결과 목록
        total_elapsed: 전체 소요 시간 (초)
    """
    logger = logging.getLogger("CrawlerOrchestrator")

    success_results = [r for r in results if r.success]
    failed_results = [r for r in results if not r.success]

    total_count = sum(r.count for r in success_results)

    logger.info("")
    logger.info("=" * 60)
    logger.info("실행 결과 요약")
    logger.info("=" * 60)

    if success_results:
        logger.info("")
        logger.info(f"[성공] {len(success_results)}개 수집기")
        logger.info("-" * 40)
        for r in sorted(success_results, key=lambda x: x.name):
            logger.info(
                f"  - {r.name:<20} : {r.count:>8}건 ({r.elapsed_seconds:.1f}초)"
            )

    if failed_results:
        logger.info("")
        logger.info(f"[실패] {len(failed_results)}개 수집기")
        logger.info("-" * 40)
        for r in sorted(failed_results, key=lambda x: x.name):
            error_msg = (r.error or "Unknown error")[:50]
            logger.info(f"  - {r.name:<20} : {error_msg}")

    logger.info("")
    logger.info("=" * 60)
    logger.info("전체 통계")
    logger.info("=" * 60)
    logger.info(f"  실행 수집기    : {len(results)}개")
    logger.info(f"  성공           : {len(success_results)}개")
    logger.info(f"  실패           : {len(failed_results)}개")
    logger.info(f"  총 수집 건수   : {total_count:,}건")
    logger.info(f"  전체 소요 시간 : {total_elapsed:.1f}초")
    logger.info("=" * 60)


def parse_args() -> argparse.Namespace:
    """
    명령줄 인자 파싱

    Returns:
        파싱된 인자 객체
    """
    parser = argparse.ArgumentParser(
        description="DWH 데이터 수집기 오케스트레이터",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
사용 예시:
  python main.py                              # 전체 수집기 실행
  python main.py --collectors pharmacy_nic    # 특정 수집기만 실행
  python main.py --dry-run                    # 실제 실행 없이 확인
  python main.py --verbose                    # 상세 로그 출력

사용 가능한 수집기:
  pharmacy_nic          - 국립중앙의료원 약국 정보
  pharmacy_hira         - 건강보험심사평가원 약국 정보
  hospital_nic          - 국립중앙의료원 병원 정보
  hospital_hira         - 건강보험심사평가원 병원 정보
  store_semas           - 소상공인진흥공단 상가 정보
  medical_type          - 의료기관종별 정보
  health_stat           - 보건의료 통계

  building_ledger       - 건축물대장 (면적 정보)
  population_stat       - 주민등록 인구/세대 통계
  business_status       - 의료기관 운영상태 (폐업/휴업)
  commercial_zone       - 상권 영역 정보
  medical_department    - 의료기관 진료과목 상세
  prescription_stat     - 의약품 처방 통계
        """,
    )

    parser.add_argument(
        "--collectors",
        "-c",
        nargs="+",
        choices=DEFAULT_COLLECTORS,
        default=None,
        metavar="NAME",
        help="실행할 수집기 이름 (기본값: 전체 실행)",
    )

    parser.add_argument(
        "--dry-run",
        "-d",
        action="store_true",
        help="실제 수집 없이 설정 확인만 수행",
    )

    parser.add_argument(
        "--workers",
        "-w",
        type=int,
        default=MAX_WORKERS,
        help=f"병렬 워커 수 (기본값: {MAX_WORKERS})",
    )

    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="상세 로그 출력 (DEBUG 레벨)",
    )

    parser.add_argument(
        "--list",
        "-l",
        action="store_true",
        help="사용 가능한 수집기 목록 출력 후 종료",
    )

    return parser.parse_args()


def list_collectors() -> None:
    """사용 가능한 수집기 목록 출력"""
    print("\n사용 가능한 수집기 목록:")
    print("-" * 50)

    descriptions = {
        "pharmacy_nic": "국립중앙의료원 약국 정보",
        "pharmacy_hira": "건강보험심사평가원 약국 정보",
        "hospital_nic": "국립중앙의료원 병원 정보",
        "hospital_hira": "건강보험심사평가원 병원 정보",
        "store_semas": "소상공인진흥공단 상가 정보",
        "medical_type": "의료기관종별 정보",
        "health_stat": "보건의료 통계",

        "building_ledger": "건축물대장 (면적 정보)",
        "population_stat": "주민등록 인구/세대 통계",
        "business_status": "의료기관 운영상태 (폐업/휴업)",
        "commercial_zone": "상권 영역 정보 (골목/발달/전통시장)",
        "medical_department": "의료기관 진료과목 상세",
        "prescription_stat": "의약품 처방 통계",
    }

    for name in DEFAULT_COLLECTORS:
        desc = descriptions.get(name, "")
        collector_class = import_collector_class(name)
        status = "[OK]" if collector_class else "[NOT IMPLEMENTED]"
        print(f"  {name:<20} {status:<18} {desc}")

    print("-" * 50)
    print(f"총 {len(DEFAULT_COLLECTORS)}개 수집기\n")


def main() -> int:
    """
    메인 함수

    Returns:
        종료 코드 (0: 성공, 1: 실패)
    """
    args = parse_args()

    logger = setup_logging(verbose=args.verbose)

    if args.list:
        list_collectors()
        return 0

    if args.workers < 1:
        logger.error(f"워커 수는 1 이상이어야 합니다: {args.workers}")
        return 1

    collectors_to_run = args.collectors or DEFAULT_COLLECTORS
    valid_collectors = []
    for name in collectors_to_run:
        if name in COLLECTOR_REGISTRY:
            valid_collectors.append(name)
        else:
            logger.warning(f"알 수 없는 수집기: {name}")

    if not valid_collectors:
        logger.error("실행할 수집기가 없습니다.")
        return 1

    overall_start = time.time()

    results = run_parallel(
        collectors=valid_collectors,
        max_workers=args.workers,
        dry_run=args.dry_run,
    )

    overall_elapsed = time.time() - overall_start

    print_summary(results, overall_elapsed)

    failed_count = sum(1 for r in results if not r.success)
    return 1 if failed_count > 0 else 0


if __name__ == "__main__":
    sys.exit(main())
