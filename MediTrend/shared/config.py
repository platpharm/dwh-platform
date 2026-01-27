"""환경 설정 모듈"""
import os
from dataclasses import dataclass
from typing import Optional


@dataclass
class ElasticsearchConfig:
    """Elasticsearch 설정"""
    host: str = os.getenv("ES_HOST", "host.docker.internal")
    port: int = int(os.getenv("ES_PORT", "54321"))
    scheme: str = os.getenv("ES_SCHEME", "http")
    username: Optional[str] = os.getenv("ES_USERNAME")
    password: Optional[str] = os.getenv("ES_PASSWORD")

    @property
    def url(self) -> str:
        """ES URL 반환"""
        return f"{self.scheme}://{self.host}:{self.port}"

    @property
    def hosts(self) -> list:
        """ES hosts 리스트 반환 (URL 문자열 형식)"""
        return [self.url]


# 인덱스 Alias 정의
class ESIndex:
    """Elasticsearch 인덱스 Alias"""
    # CDC Source data (platpharm)
    CDC_ORDERS_DETAIL = "platpharm.public.orders_detail"
    CDC_PRODUCT = "platpharm.public.product"
    CDC_ACCOUNT = "platpharm.public.account"

    # Raw data
    TREND_RAW = "medi-trend-raw"
    TREND_DATA = "medi-trend-trend-data"

    # Preprocessed data
    PREPROCESSED_ORDER = "medi-trend-preprocessed-order"
    PREPROCESSED_PRODUCT = "medi-trend-preprocessed-product"
    PREPROCESSED_PHARMACY = "medi-trend-preprocessed-pharmacy"

    # Mapping and results
    TREND_PRODUCT_MAPPING = "medi-trend-product-mapping"
    CLUSTERING_RESULT = "medi-trend-clustering-result"
    FORECASTING_RESULT = "medi-trend-forecasting-result"
    RANKING_RESULT = "medi-trend-ranking-result"
    TARGETING_RESULT = "medi-trend-targeting-result"
    VECTOR_STORE = "medi-trend-vectors"


# 싱글톤 설정 인스턴스
es_config = ElasticsearchConfig()
