"""네이버 데이터랩 크롤러"""

import os
import hashlib
import hmac
import base64
import time
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential

from shared.clients.es_client import es_client
from shared.models.schemas import TrendData
from shared.config import ESIndex


class NaverDatalabCrawler:
    """네이버 데이터랩 API 크롤러"""

    BASE_URL = "https://openapi.naver.com/v1/datalab/search"

    def __init__(self):
        self.client_id = os.getenv("NAVER_CLIENT_ID", "")
        self.client_secret = os.getenv("NAVER_CLIENT_SECRET", "")

    def _get_headers(self) -> Dict[str, str]:
        """API 요청 헤더 생성"""
        return {
            "X-Naver-Client-Id": self.client_id,
            "X-Naver-Client-Secret": self.client_secret,
            "Content-Type": "application/json",
        }

    def _build_request_body(
        self,
        keywords: List[str],
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        time_unit: str = "date",
    ) -> Dict[str, Any]:
        """API 요청 바디 생성"""
        if not start_date:
            start_date = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
        if not end_date:
            end_date = datetime.now().strftime("%Y-%m-%d")

        keyword_groups = [
            {"groupName": kw, "keywords": [kw]} for kw in keywords
        ]

        return {
            "startDate": start_date,
            "endDate": end_date,
            "timeUnit": time_unit,
            "keywordGroups": keyword_groups,
        }

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
    )
    async def fetch_trends(
        self,
        keywords: List[str],
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """네이버 데이터랩 트렌드 데이터 가져오기"""
        if not self.client_id or not self.client_secret:
            raise ValueError("NAVER_CLIENT_ID and NAVER_CLIENT_SECRET must be set")

        # 최대 5개 키워드까지만 한 번에 요청 가능
        results = []
        for i in range(0, len(keywords), 5):
            batch_keywords = keywords[i : i + 5]
            body = self._build_request_body(batch_keywords, start_date, end_date)

            async with httpx.AsyncClient() as client:
                response = await client.post(
                    self.BASE_URL,
                    headers=self._get_headers(),
                    json=body,
                    timeout=30.0,
                )
                response.raise_for_status()
                data = response.json()

                if "results" in data:
                    results.extend(data["results"])

            # Rate limiting
            if i + 5 < len(keywords):
                time.sleep(0.5)

        return results

    def _parse_trend_data(
        self, raw_results: List[Dict[str, Any]]
    ) -> List[TrendData]:
        """API 응답을 TrendData 스키마로 변환"""
        trend_data_list = []

        for result in raw_results:
            keyword = result.get("title", "")
            data_points = result.get("data", [])

            for point in data_points:
                period = point.get("period", "")
                ratio = point.get("ratio", 0)

                try:
                    timestamp = datetime.strptime(period, "%Y-%m-%d")
                except ValueError:
                    timestamp = datetime.now()

                trend_data = TrendData(
                    keyword=keyword,
                    source="naver",
                    score=float(ratio),
                    timestamp=timestamp,
                    raw_data={
                        "period": period,
                        "ratio": ratio,
                        "keywords": result.get("keywords", []),
                    },
                )
                trend_data_list.append(trend_data)

        return trend_data_list

    async def crawl_and_save(
        self,
        keywords: List[str],
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> int:
        """크롤링 후 ES에 저장"""
        raw_results = await self.fetch_trends(keywords, start_date, end_date)
        trend_data_list = self._parse_trend_data(raw_results)

        if not trend_data_list:
            return 0

        # TrendData를 dict로 변환
        documents = [
            {
                **data.model_dump(),
                "timestamp": data.timestamp.isoformat(),
            }
            for data in trend_data_list
        ]

        # ES에 bulk 저장
        success, _ = es_client.bulk_index(
            index=ESIndex.TREND_DATA,
            documents=documents,
        )

        return success


# 싱글톤 인스턴스
naver_crawler = NaverDatalabCrawler()
