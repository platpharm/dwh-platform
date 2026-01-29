import os
from dataclasses import dataclass
from typing import Optional

@dataclass
class ElasticsearchConfig:
    host: str = os.getenv("ES_HOST", "host.docker.internal")
    port: int = int(os.getenv("ES_PORT", "54321"))
    scheme: str = os.getenv("ES_SCHEME", "https")
    username: Optional[str] = os.getenv("ES_USERNAME")
    password: Optional[str] = os.getenv("ES_PASSWORD")

    @property
    def url(self) -> str:
        return f"{self.scheme}://{self.host}:{self.port}"

    @property
    def hosts(self) -> list:
        return [self.url]

class ESIndex:
    CDC_ORDERS = "platpharm.public.orders"
    CDC_ORDERS_DETAIL = "platpharm.public.orders_detail"
    CDC_PRODUCT = "platpharm.public.product"
    CDC_ACCOUNT = "platpharm.public.account"
    CDC_PRODUCT_KIMS_MAPPING = "platpharm.public.product_kims_mapping"
    CDC_KIMS = "platpharm.public.kims_edis_indexdb"

    TREND_RAW = "medi-db-raw"
    TREND_DATA = "medi-db-trend-data"

    PREPROCESSED_ORDER = "medi-db-preprocessed-order"
    PREPROCESSED_PRODUCT = "medi-db-preprocessed-product"
    PREPROCESSED_PHARMACY = "medi-db-preprocessed-pharmacy"

    TREND_PRODUCT_MAPPING = "medi-db-product-mapping"
    CLUSTERING_RESULT = "medi-db-clustering-result"
    FORECASTING_RESULT = "medi-db-forecasting-result"
    RANKING_RESULT = "medi-db-ranking-result"
    TARGETING_RESULT = "medi-db-targeting-result"
    VECTOR_STORE = "medi-db-vectors"

es_config = ElasticsearchConfig()
