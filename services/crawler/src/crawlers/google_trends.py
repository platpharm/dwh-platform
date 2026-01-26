"""구글 트렌드 크롤러 (pytrends 사용)"""

import time
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional

from pytrends.request import TrendReq
from tenacity import retry, stop_after_attempt, wait_exponential

from shared.clients.es_client import es_client
from shared.models.schemas import TrendData
from shared.config import ESIndex


class GoogleTrendsCrawler:
    """구글 트렌드 크롤러 (pytrends 라이브러리 사용)"""

    def __init__(self, hl: str = "ko", tz: int = 540, geo: str = "KR"):
        """
        Args:
            hl: 언어 설정 (default: 한국어)
            tz: 타임존 오프셋 (default: 540 = UTC+9, 한국)
            geo: 지역 설정 (default: 한국)
        """
        self.hl = hl
        self.tz = tz
        self.geo = geo
        self.pytrends = TrendReq(hl=hl, tz=tz)

    def _get_timeframe(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> str:
        """timeframe 문자열 생성"""
        if not start_date:
            start_date = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
        if not end_date:
            end_date = datetime.now().strftime("%Y-%m-%d")

        return f"{start_date} {end_date}"

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
    )
    def fetch_interest_over_time(
        self,
        keywords: List[str],
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> Dict[str, Any]:
        """시간에 따른 관심도 데이터 가져오기"""
        timeframe = self._get_timeframe(start_date, end_date)
        results = {}

        # pytrends는 한 번에 최대 5개 키워드만 처리 가능
        for i in range(0, len(keywords), 5):
            batch_keywords = keywords[i : i + 5]

            self.pytrends.build_payload(
                kw_list=batch_keywords,
                cat=0,  # 모든 카테고리
                timeframe=timeframe,
                geo=self.geo,
            )

            interest_df = self.pytrends.interest_over_time()

            if not interest_df.empty:
                # isPartial 컬럼 제거
                if "isPartial" in interest_df.columns:
                    interest_df = interest_df.drop(columns=["isPartial"])

                for keyword in batch_keywords:
                    if keyword in interest_df.columns:
                        results[keyword] = interest_df[keyword].to_dict()

            # Rate limiting
            if i + 5 < len(keywords):
                time.sleep(1)

        return results

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
    )
    def fetch_related_queries(
        self,
        keywords: List[str],
    ) -> Dict[str, Dict[str, Any]]:
        """연관 검색어 가져오기"""
        results = {}

        for i in range(0, len(keywords), 5):
            batch_keywords = keywords[i : i + 5]

            self.pytrends.build_payload(
                kw_list=batch_keywords,
                cat=0,
                timeframe="today 3-m",
                geo=self.geo,
            )

            related = self.pytrends.related_queries()

            for keyword in batch_keywords:
                if keyword in related:
                    keyword_data = related[keyword]
                    results[keyword] = {
                        "top": (
                            keyword_data["top"].to_dict("records")
                            if keyword_data["top"] is not None
                            else []
                        ),
                        "rising": (
                            keyword_data["rising"].to_dict("records")
                            if keyword_data["rising"] is not None
                            else []
                        ),
                    }

            # Rate limiting
            if i + 5 < len(keywords):
                time.sleep(1)

        return results

    def _parse_trend_data(
        self,
        interest_data: Dict[str, Dict],
        related_data: Optional[Dict[str, Dict]] = None,
    ) -> List[TrendData]:
        """API 응답을 TrendData 스키마로 변환"""
        trend_data_list = []

        for keyword, time_series in interest_data.items():
            for timestamp, score in time_series.items():
                # timestamp가 pandas Timestamp인 경우 처리
                if hasattr(timestamp, "to_pydatetime"):
                    dt = timestamp.to_pydatetime()
                elif isinstance(timestamp, str):
                    dt = datetime.fromisoformat(timestamp)
                else:
                    dt = timestamp

                raw_data = {"score": score}

                # 연관 검색어 추가
                if related_data and keyword in related_data:
                    raw_data["related_queries"] = related_data[keyword]

                trend_data = TrendData(
                    keyword=keyword,
                    source="google",
                    score=float(score),
                    timestamp=dt,
                    raw_data=raw_data,
                )
                trend_data_list.append(trend_data)

        return trend_data_list

    def crawl_and_save(
        self,
        keywords: List[str],
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        include_related: bool = True,
    ) -> int:
        """크롤링 후 ES에 저장"""
        interest_data = self.fetch_interest_over_time(keywords, start_date, end_date)

        related_data = None
        if include_related:
            related_data = self.fetch_related_queries(keywords)

        trend_data_list = self._parse_trend_data(interest_data, related_data)

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
google_crawler = GoogleTrendsCrawler()
