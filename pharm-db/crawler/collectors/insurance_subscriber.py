"""
건강보험 직장 부과자료 통계 수집기 (Insurance Subscriber Collector)

데이터 소스: 국민건강보험공단 직장 부과자료관리
수집 방식: data.go.kr CSV 파일 다운로드
공공데이터 PK: 15072694
파일명: 국민건강보험공단_직장 부과자료관리_20241231.csv

수집 데이터:
    - 연도별 직장가입자 수, 적용인구 수
    - 보험료 총액 (백만원)
    - 가입자/적용인구 1인당 월평균 보험료 (원)

주의: data.go.kr의 atchFileId는 데이터 업데이트 시 변경될 수 있습니다.
다운로드 실패 시 아래 URL에서 최신 파일 ID를 확인하세요:
https://www.data.go.kr/data/15072694/fileData.do

ES 인덱스: insurance_subscriber
"""

import csv
import io
from datetime import datetime
from typing import Any, Dict, List, Optional

from crawler.base_collector import BaseCollector
from crawler.config import CONFIG


FILE_URLS = {
    "workplace_levy": (
        "https://www.data.go.kr/cmm/cmm/fileDownload.do"
        "?atchFileId=FILE_000000003241510&fileDetailSn=1&insertDataPrcus=N"
    ),
}


class InsuranceSubscriberCollector(BaseCollector):
    """
    건강보험 직장 부과자료 통계 수집기

    국민건강보험공단의 연도별 직장 건강보험 부과 통계 CSV를
    다운로드하여 파싱합니다.

    주의: atchFileId가 변경되어 다운로드 실패 시
    https://www.data.go.kr/data/15072694/fileData.do 에서
    최신 파일 ID를 확인하세요.
    """

    ES_INDEX = "insurance_subscriber"

    def __init__(self):
        super().__init__(name="InsuranceSubscriberCollector")
        self.service_key = CONFIG["data_go_kr"]["service_key"]

    def collect(self) -> List[Dict[str, Any]]:
        """
        건강보험 직장 부과자료 CSV 수집

        Returns:
            수집된 통계 리스트
        """
        all_data = []

        self.logger.info("건강보험 직장 부과자료 CSV 다운로드 시작")
        data = self._collect_workplace_levy_csv()
        all_data.extend(data)
        self.logger.info(
            f"건강보험 직장 부과자료 수집 완료: {len(data)}건"
        )

        self.logger.info(f"전체 수집 완료: 총 {len(all_data)}건")
        return all_data

    def _download_csv(
        self, url: str, encoding: str = "euc-kr"
    ) -> List[Dict[str, str]]:
        self.logger.info(f"CSV 다운로드: {url[:80]}...")

        response = self._make_request(url=url, timeout=60)
        if response is None:
            self.logger.error("CSV 파일 다운로드 실패")
            return []

        content_type = response.headers.get("Content-Type", "")
        if "text/html" in content_type:
            self.logger.error(
                "CSV 대신 HTML 응답 수신 - 파일 ID가 변경되었을 수 있습니다. "
                "data.go.kr에서 최신 atchFileId를 확인하세요: "
                "https://www.data.go.kr/data/15072694/fileData.do"
            )
            return []

        try:
            text = response.content.decode(encoding)
        except UnicodeDecodeError:
            try:
                text = response.content.decode("utf-8")
            except UnicodeDecodeError:
                text = response.content.decode("cp949")

        reader = csv.DictReader(io.StringIO(text))
        rows = list(reader)
        self.logger.info(f"CSV 파싱 완료: {len(rows)}행")
        return rows

    def _collect_workplace_levy_csv(self) -> List[Dict[str, Any]]:
        rows = self._download_csv(FILE_URLS["workplace_levy"])
        if not rows:
            return []

        collected_at = datetime.now().isoformat()
        results = []

        sample_keys = list(rows[0].keys()) if rows else []
        self.logger.info(f"CSV 컬럼: {sample_keys}")

        for row in rows:
            year = row.get("연도", "").strip()

            processed = {
                "stat_type": "workplace_levy",
                "year": year,
                "insurance_premium_mln": self._safe_int(
                    row.get("보험료(백만원)")
                ),
                "avg_monthly_premium_per_subscriber": self._safe_int(
                    row.get("가입자 1인당 월평균보험료(원)")
                ),
                "avg_monthly_premium_per_population": self._safe_int(
                    row.get("적용인구 1인당 월평균보험료(원)")
                ),
                "workplace_subscriber_cnt": self._safe_int(
                    row.get("직장가입자(명)")
                ),
                "applied_population_cnt": self._safe_int(
                    row.get("적용인구(명)")
                ),
                "collected_at": collected_at,
                "_doc_id": f"{year}_workplace_levy",
            }

            results.append(processed)

        return results

    def _safe_int(self, value: Any) -> Optional[int]:
        if value is None or value == "":
            return None
        try:
            return int(str(value).strip().replace(",", ""))
        except (ValueError, TypeError):
            return None

    def run(self) -> Dict[str, int]:
        """
        수집 및 Elasticsearch 저장 실행

        Returns:
            저장 결과 통계
        """
        self.logger.info("InsuranceSubscriberCollector 실행 시작")

        data = self.collect()

        if not data:
            self.logger.warning("수집된 건강보험 가입자 데이터가 없습니다.")
            return {"success": 0, "failed": 0}

        result = self.save_to_es(
            data=data,
            index_name=self.ES_INDEX,
            id_field="_doc_id"
        )

        self.logger.info(
            f"InsuranceSubscriberCollector 실행 완료: "
            f"성공={result['success']}, 실패={result['failed']}"
        )

        return result


if __name__ == "__main__":
    collector = InsuranceSubscriberCollector()
    collector.run()
