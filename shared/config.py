"""환경 설정 모듈"""
import os
from dataclasses import dataclass
from typing import Optional


@dataclass
class PostgreSQLConfig:
    """PostgreSQL 설정"""
    host: str = os.getenv("PG_HOST", "host.docker.internal")
    port: int = int(os.getenv("PG_PORT", "12345"))
    database: str = os.getenv("PG_DATABASE", "platpharm")
    user: str = os.getenv("PG_USER", "postgres")
    password: str = os.getenv("PG_PASSWORD", "")

    @property
    def connection_string(self) -> str:
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"


@dataclass
class ElasticsearchConfig:
    """Elasticsearch 설정"""
    host: str = os.getenv("ES_HOST", "host.docker.internal")
    port: int = int(os.getenv("ES_PORT", "54321"))
    scheme: str = os.getenv("ES_SCHEME", "http")
    username: Optional[str] = os.getenv("ES_USERNAME")
    password: Optional[str] = os.getenv("ES_PASSWORD")

    @property
    def hosts(self) -> list:
        return [{"host": self.host, "port": self.port, "scheme": self.scheme}]


# 인덱스 Alias 정의
class ESIndex:
    """Elasticsearch 인덱스 Alias"""
    TREND_DATA = "medi-trend-trend-data"
    TREND_PRODUCT_MAPPING = "medi-trend-product-mapping"
    CLUSTERING_RESULT = "medi-trend-clustering-result"
    FORECASTING_RESULT = "medi-trend-forecasting-result"
    RANKING_RESULT = "medi-trend-ranking-result"
    TARGETING_RESULT = "medi-trend-targeting-result"


# 싱글톤 설정 인스턴스
pg_config = PostgreSQLConfig()
es_config = ElasticsearchConfig()
