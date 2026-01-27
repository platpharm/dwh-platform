"""Clustering Service - FastAPI 엔트리포인트"""
import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from shared.clients.es_client import es_client
from shared.config import ESIndex

from .api import router

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def get_expected_mappings():
    """Expected index mappings for clustering results"""
    return {
        "properties": {
            "entity_type": {"type": "keyword"},
            "entity_id": {"type": "keyword"},  # keyword supports both int and string IDs
            "cluster_id": {"type": "integer"},
            "algorithm": {"type": "keyword"},
            "features": {"type": "object", "enabled": True},
            "umap_coords": {"type": "float"},
            "timestamp": {"type": "date"}
        }
    }


def ensure_index_exists():
    """클러스터링 결과 인덱스 생성 또는 재생성 (매핑 불일치 시)"""
    index_name = ESIndex.CLUSTERING_RESULT
    mappings = get_expected_mappings()
    settings = {
        "number_of_shards": 1,
        "number_of_replicas": 0
    }

    if not es_client.index_exists(index_name):
        es_client.create_index(
            index=index_name,
            mappings=mappings,
            settings=settings
        )
        logger.info(f"Created index: {index_name}")
    else:
        # Check if entity_id mapping is correct (should be 'long', not 'integer')
        try:
            current_mapping = es_client.client.indices.get_mapping(index=index_name)
            entity_id_type = (
                current_mapping[index_name]["mappings"]
                .get("properties", {})
                .get("entity_id", {})
                .get("type")
            )

            if entity_id_type in ("integer", "long"):
                logger.warning(
                    f"Index {index_name} has entity_id mapped as '{entity_id_type}', "
                    "but 'keyword' is required for both product and pharmacy IDs. Recreating index..."
                )
                # Delete and recreate with correct mapping
                es_client.client.indices.delete(index=index_name)
                es_client.create_index(
                    index=index_name,
                    mappings=mappings,
                    settings=settings
                )
                logger.info(f"Recreated index: {index_name} with correct mapping")
        except Exception as e:
            logger.error(f"Error checking/updating index mapping: {e}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """애플리케이션 생명주기 관리"""
    # Startup
    logger.info("Starting Clustering Service...")

    # ES 연결 확인
    if es_client.health_check():
        logger.info("Elasticsearch connection established")
        ensure_index_exists()
    else:
        logger.warning("Elasticsearch connection failed - service may be degraded")

    yield

    # Shutdown
    logger.info("Shutting down Clustering Service...")


# FastAPI 앱 생성
app = FastAPI(
    title="MediTrend Clustering Service",
    description="상품 및 약국 클러스터링 서비스 (HDBSCAN, K-Prototype, GMM, Mini-Batch K-Means)",
    version="0.1.0",
    lifespan=lifespan
)

# CORS 설정
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 라우터 등록
app.include_router(router, tags=["clustering"])


@app.get("/")
async def root():
    """서비스 정보"""
    return {
        "service": "MediTrend Clustering Service",
        "version": "0.1.0",
        "algorithms": ["hdbscan", "k_prototype", "gmm", "minibatch_kmeans"],
        "entity_types": ["product", "pharmacy"],
        "endpoints": {
            "cluster_run": "POST /cluster/run",
            "cluster_hdbscan": "POST /cluster/hdbscan",
            "cluster_gmm": "POST /cluster/gmm",
            "cluster_kmeans": "POST /cluster/kmeans",
            "cluster_results": "GET /cluster/results",
            "health": "GET /health"
        }
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
