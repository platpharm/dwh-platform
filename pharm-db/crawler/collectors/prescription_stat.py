"""
의약품 처방 통계 수집기 (Prescription Statistics Collector)

데이터 소스: 의약품 처방전 세부사항 통계 (CSV 파일 다운로드)
공공데이터 PK: 15007117
다운로드 URL: data.go.kr fileDownload (atchFileId=FILE_000000003203901)

의약품처방정보 API(mdcinPrscrbInfoService)는 존재하지 않는 서비스이며,
의약품사용정보 API(msupUserInfoService1.2)는 health_stat 수집기에서 이미 사용 중입니다.
이 수집기는 CSV 파일 다운로드 방식으로 처방 통계를 수집합니다.

ES 인덱스: prescription_stat
"""

import csv
import io
from datetime import datetime
from typing import Any, Dict, List, Optional

from crawler.base_collector import BaseCollector
from crawler.config import CONFIG


FILE_URLS = {
    "prescription_detail": (
        "https://www.data.go.kr/cmm/cmm/fileDownload.do"
        "?atchFileId=FILE_000000003203901&fileDetailSn=1&insertDataPrcus=N"
    ),
}


class PrescriptionStatCollector(BaseCollector):
    """
    의약품 처방 통계 수집기

    data.go.kr에서 CSV 파일을 다운로드하여 처방 통계를 수집합니다.
    """

    ES_INDEX = "prescription_stat"

    def __init__(self):
        super().__init__(name="PrescriptionStatCollector")
        self.service_key = CONFIG["data_go_kr"]["service_key"]

    def collect(self) -> List[Dict[str, Any]]:
        """
        의약품 처방 통계 CSV 수집

        Returns:
            수집된 처방 통계 리스트
        """
        all_data = []

        self.logger.info("의약품 처방통계 CSV 다운로드 시작")
        prescription_data = self._collect_prescription_csv()
        all_data.extend(prescription_data)
        self.logger.info(
            f"의약품 처방통계 수집 완료: {len(prescription_data)}건"
        )

        self.logger.info(f"전체 처방통계 수집 완료: 총 {len(all_data)}건")
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
                "CSV 대신 HTML 응답 수신 - 파일 ID가 변경되었거나 로그인 필요. "
                "data.go.kr에서 최신 atchFileId를 확인하세요: "
                "https://www.data.go.kr/data/15007117/fileData.do"
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

    def _collect_prescription_csv(self) -> List[Dict[str, Any]]:
        rows = self._download_csv(FILE_URLS["prescription_detail"])
        if not rows:
            return []

        collected_at = datetime.now().isoformat()
        results = []

        sample_keys = list(rows[0].keys()) if rows else []
        self.logger.info(f"CSV 컬럼: {sample_keys[:10]}")

        for row in rows:
            diag_ym = (
                row.get("진료년월", row.get("diagYm", ""))
            ).strip()
            sido_nm = (
                row.get("시도", row.get("시도명", row.get("sidoCdNm", "")))
            ).strip()
            sggu_nm = (
                row.get("시군구", row.get("시군구명", row.get("sgguCdNm", "")))
            ).strip()
            mdcin_nm = (
                row.get("의약품명", row.get("약품명", row.get("gnlNmNm", "")))
            ).strip()

            processed = {
                "stat_type": "prescription",
                "diag_ym": diag_ym,
                "sido_nm": sido_nm,
                "sggu_nm": sggu_nm,
                "mdcin_nm": mdcin_nm,
                "mdcin_cd": (
                    row.get("의약품코드", row.get("약품코드", row.get("gnlNmCd", "")))
                ).strip(),
                "prscrpt_cnt": self._safe_int(
                    row.get("처방건수", row.get("prscrptCnt"))
                ),
                "prscrpt_amt": self._safe_int(
                    row.get("처방금액", row.get("prscrptAmt"))
                ),
                "tot_use_qty": self._safe_float(
                    row.get("총사용량", row.get("totUseQty"))
                ),
                "collected_at": collected_at,
            }

            processed["_doc_id"] = (
                f"{diag_ym}_{sido_nm}_{sggu_nm}_{mdcin_nm}_prescription"
            )
            results.append(processed)

        return results

    def _safe_int(self, value: Any) -> Optional[int]:
        if value is None or value == "":
            return None
        try:
            return int(str(value).strip().replace(",", ""))
        except (ValueError, TypeError):
            return None

    def _safe_float(self, value: Any) -> Optional[float]:
        if value is None or value == "":
            return None
        try:
            return float(str(value).strip().replace(",", ""))
        except (ValueError, TypeError):
            return None

    def run(self) -> Dict[str, int]:
        """
        수집 및 Elasticsearch 저장 실행

        Returns:
            저장 결과 통계
        """
        self.logger.info("PrescriptionStatCollector 실행 시작")

        data = self.collect()

        if not data:
            self.logger.warning("수집된 처방통계 데이터가 없습니다.")
            return {"success": 0, "failed": 0}

        result = self.save_to_es(
            data=data,
            index_name=self.ES_INDEX,
            id_field="_doc_id"
        )

        self.logger.info(
            f"PrescriptionStatCollector 실행 완료: "
            f"성공={result['success']}, 실패={result['failed']}"
        )

        return result


if __name__ == "__main__":
    collector = PrescriptionStatCollector()
    collector.run()
