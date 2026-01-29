import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from shared.clients.es_client import es_client
from shared.config import ESIndex

from .api import router
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

def get_expected_mappings():
    return {
        "properties": {
            "entity_type": {"type": "keyword"},
            "entity_id": {"type": "keyword"},
            "entity_name": {"type": "keyword"},
            "cluster_id": {"type": "integer"},
            "algorithm": {"type": "keyword"},
            "features": {"type": "object", "enabled": True},
            "umap_coords": {"type": "float"},
            "timestamp": {"type": "date"}
        }
    }

def ensure_index_exists():
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
    logger.info("Starting Clustering Service...")

    if es_client.health_check():
        logger.info("Elasticsearch connection established")
        ensure_index_exists()
    else:
        logger.warning("Elasticsearch connection failed - service may be degraded")

    yield

    logger.info("Shutting down Clustering Service...")

app = FastAPI(
    title="MediDB Clustering Service",
    description="상품 및 약국 클러스터링 서비스 (HDBSCAN, K-Prototype, GMM, Mini-Batch K-Means)",
    version="0.1.0",
    lifespan=lifespan
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(router, tags=["clustering"])

@app.get("/")
async def root():
    return {
        "service": "MediDB Clustering Service",
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
