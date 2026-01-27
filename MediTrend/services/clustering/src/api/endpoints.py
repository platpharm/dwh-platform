"""클러스터링 API 엔드포인트"""
import logging
from datetime import datetime
from typing import Dict, Any, Optional, List
import numpy as np
from fastapi import APIRouter, HTTPException, BackgroundTasks

from shared.clients.es_client import es_client
from shared.config import ESIndex
from shared.models.schemas import (
    ClusterRequest,
    ClusterResponse,
    ClusteringResult,
    HealthResponse
)

from ..algorithms import (
    HDBSCANClusterer,
    KPrototypeClusterer,
    GMMClusterer,
    MiniBatchKMeansClusterer
)

logger = logging.getLogger(__name__)
router = APIRouter()

clustering_jobs: Dict[str, Dict[str, Any]] = {}


def _fetch_product_data() -> tuple:
    """ES에서 상품 데이터 조회

    TREND_PRODUCT_MAPPING에서 trend_score, match_score를 조회하고
    CDC_PRODUCT에서 상품명 등 추가 정보를 조인
    """
    trend_data = {}
    for p in es_client.scroll_search(
        index=ESIndex.TREND_PRODUCT_MAPPING,
        query={"match_all": {}},
        size=1000
    ):
        product_id = p.get("product_id")
        if product_id:
            trend_data[int(product_id)] = {
                "trend_score": p.get("trend_score", 0),
                "match_score": p.get("match_score", 0),
            }

    logger.info(f"Loaded {len(trend_data)} products from TREND_PRODUCT_MAPPING")

    product_info = {}
    for p in es_client.scroll_search(
        index=ESIndex.CDC_PRODUCT,
        query={"match_all": {}},
        size=1000
    ):
        product_id = p.get("id")
        if product_id:
            try:
                product_id_int = int(product_id)
            except (ValueError, TypeError):
                continue
            product_info[product_id_int] = {
                "product_name": p.get("name", ""),
                "category1": p.get("category1", ""),
                "category2": p.get("category2", ""),
                "category3": p.get("category3", ""),
            }

    logger.info(f"Loaded {len(product_info)} products from CDC_PRODUCT")

    ids = []
    features = []
    raw_data = []

    for product_id, scores in trend_data.items():
        trend_score = float(scores.get("trend_score", 0))
        match_score = float(scores.get("match_score", 0))

        info = product_info.get(product_id, {})
        product_name = info.get("product_name", "")

        if not product_name:
            continue

        ids.append(product_id)

        feature_vector = [
            trend_score,
            match_score,
        ]
        features.append(feature_vector)

        raw_data.append({
            "product_id": product_id,
            "product_name": product_name,
            "trend_score": trend_score,
            "match_score": match_score,
            "category1": info.get("category1", ""),
            "category2": info.get("category2", ""),
            "category3": info.get("category3", ""),
        })

    logger.info(f"Prepared {len(ids)} products for clustering (with names)")

    if not ids:
        return [], [], []

    return ids, np.array(features), raw_data


def _fetch_pharmacy_data() -> tuple:
    """ES에서 약국 데이터 조회 (전처리된 데이터)"""
    query = {"match_all": {}}

    try:
        pharmacies = es_client.search(
            index="medi-trend-preprocessed-pharmacy",
            query=query,
            size=10000
        )
    except Exception:
        logger.warning("Preprocessed pharmacy index not found")
        return [], [], []

    if not pharmacies:
        return [], [], []

    ids = []
    features = []
    raw_data = []

    for p in pharmacies:
        ids.append(p.get("pharmacy_id") or p.get("account_id"))

        feature_vector = [
            float(p.get("total_orders", 0)),
            float(p.get("avg_order_amount", 0)),
            float(p.get("order_frequency", 0)),
            float(p.get("unique_products", 0)),
        ]
        features.append(feature_vector)
        raw_data.append(p)

    return ids, np.array(features), raw_data


def _save_clustering_results(
    entity_type: str,
    entity_ids: List,  # int for products, str for pharmacies
    algorithm: str,
    cluster_results: List[Dict[str, Any]],
    raw_data: List[Dict[str, Any]]
) -> int:
    """클러스터링 결과를 ES에 저장"""
    timestamp = datetime.utcnow()
    documents = []

    for i, (entity_id, result, raw) in enumerate(zip(entity_ids, cluster_results, raw_data)):
            if entity_type == "product":
            entity_name = raw.get("product_name") or raw.get("name")
        else:  # pharmacy
            entity_name = raw.get("name") or raw.get("host_name")

        if not entity_name:
            continue

        doc = ClusteringResult(
            entity_type=entity_type,
            entity_id=entity_id,
            entity_name=entity_name,
            cluster_id=result["cluster_id"],
            algorithm=algorithm,
            features=raw,
            umap_coords=result.get("umap_coords"),
            timestamp=timestamp
        )
        documents.append(doc.model_dump())

    try:
        es_client.delete_by_query(
            index=ESIndex.CLUSTERING_RESULT,
            query={
                "bool": {
                    "must": [
                        {"term": {"entity_type": entity_type}},
                        {"term": {"algorithm": algorithm}}
                    ]
                }
            }
        )
    except Exception as e:
        logger.warning(f"Failed to delete old results: {e}")

    success, _ = es_client.bulk_index(
        index=ESIndex.CLUSTERING_RESULT,
        documents=documents
    )

    return success


def _run_clustering(
    algorithm: str,
    entity_type: str,
    params: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """클러스터링 실행 로직"""
    params = params or {}

    if entity_type == "product":
        entity_ids, features, raw_data = _fetch_product_data()
    elif entity_type == "pharmacy":
        entity_ids, features, raw_data = _fetch_pharmacy_data()
    else:
        raise ValueError(f"Unknown entity_type: {entity_type}")

    if len(entity_ids) == 0:
        return {
            "success": False,
            "cluster_count": 0,
            "entity_count": 0,
            "message": f"No {entity_type} data found"
        }

    if algorithm == "hdbscan":
        clusterer = HDBSCANClusterer(
            min_cluster_size=params.get("min_cluster_size", 5),
            min_samples=params.get("min_samples"),
            use_umap=params.get("use_umap", True)
        )
    elif algorithm == "k_prototype":
        clusterer = KPrototypeClusterer(
            n_clusters=params.get("n_clusters", 5)
        )
    elif algorithm == "gmm":
        clusterer = GMMClusterer(
            n_components=params.get("n_components", 5),
            covariance_type=params.get("covariance_type", "full")
        )
    elif algorithm == "minibatch_kmeans":
        clusterer = MiniBatchKMeansClusterer(
            n_clusters=params.get("n_clusters", 8),
            batch_size=params.get("batch_size", 1024)
        )
    else:
        raise ValueError(f"Unknown algorithm: {algorithm}")

    if algorithm == "k_prototype":
        categorical_indices = params.get("categorical_indices")
        clusterer.fit(features, categorical_indices)
    else:
        clusterer.fit(features)

    results = clusterer.get_results()
    summary = clusterer.get_cluster_summary()

    saved_count = _save_clustering_results(
        entity_type=entity_type,
        entity_ids=entity_ids,
        algorithm=algorithm,
        cluster_results=results,
        raw_data=raw_data
    )

    return {
        "success": True,
        "cluster_count": summary["n_clusters"],
        "entity_count": len(entity_ids),
        "message": f"Clustered {len(entity_ids)} {entity_type}s into {summary['n_clusters']} clusters",
        "summary": summary
    }


@router.post("/cluster/run", response_model=ClusterResponse)
async def run_clustering(request: ClusterRequest):
    """
    클러스터링 실행

    - algorithm: hdbscan, k_prototype, gmm, minibatch_kmeans
    - entity_type: product, pharmacy
    """
    try:
        result = _run_clustering(
            algorithm=request.algorithm,
            entity_type=request.entity_type,
            params=request.params
        )

        return ClusterResponse(
            success=result["success"],
            cluster_count=result["cluster_count"],
            entity_count=result["entity_count"],
            message=result["message"]
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Clustering error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/cluster/hdbscan", response_model=ClusterResponse)
async def run_hdbscan(
    entity_type: str = "product",
    min_cluster_size: int = 5,
    min_samples: Optional[int] = None,
    use_umap: bool = True
):
    """HDBSCAN 클러스터링 실행"""
    try:
        result = _run_clustering(
            algorithm="hdbscan",
            entity_type=entity_type,
            params={
                "min_cluster_size": min_cluster_size,
                "min_samples": min_samples,
                "use_umap": use_umap
            }
        )

        return ClusterResponse(
            success=result["success"],
            cluster_count=result["cluster_count"],
            entity_count=result["entity_count"],
            message=result["message"]
        )
    except Exception as e:
        logger.error(f"HDBSCAN error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/cluster/gmm", response_model=ClusterResponse)
async def run_gmm(
    entity_type: str = "product",
    n_components: int = 5,
    covariance_type: str = "full"
):
    """GMM 클러스터링 실행"""
    try:
        result = _run_clustering(
            algorithm="gmm",
            entity_type=entity_type,
            params={
                "n_components": n_components,
                "covariance_type": covariance_type
            }
        )

        return ClusterResponse(
            success=result["success"],
            cluster_count=result["cluster_count"],
            entity_count=result["entity_count"],
            message=result["message"]
        )
    except Exception as e:
        logger.error(f"GMM error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/cluster/kmeans", response_model=ClusterResponse)
async def run_kmeans(
    entity_type: str = "product",
    n_clusters: int = 8,
    batch_size: int = 1024
):
    """Mini-Batch K-Means 클러스터링 실행"""
    try:
        result = _run_clustering(
            algorithm="minibatch_kmeans",
            entity_type=entity_type,
            params={
                "n_clusters": n_clusters,
                "batch_size": batch_size
            }
        )

        return ClusterResponse(
            success=result["success"],
            cluster_count=result["cluster_count"],
            entity_count=result["entity_count"],
            message=result["message"]
        )
    except Exception as e:
        logger.error(f"K-Means error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/cluster/results")
async def get_clustering_results(
    entity_type: Optional[str] = None,
    algorithm: Optional[str] = None,
    cluster_id: Optional[int] = None,
    size: int = 100
):
    """
    클러스터링 결과 조회

    - entity_type: product, pharmacy (선택)
    - algorithm: hdbscan, k_prototype, gmm, minibatch_kmeans (선택)
    - cluster_id: 특정 클러스터 ID (선택)
    """
    try:
        must_clauses = []
        if entity_type:
            must_clauses.append({"term": {"entity_type": entity_type}})
        if algorithm:
            must_clauses.append({"term": {"algorithm": algorithm}})
        if cluster_id is not None:
            must_clauses.append({"term": {"cluster_id": cluster_id}})

        if must_clauses:
            query = {"bool": {"must": must_clauses}}
        else:
            query = {"match_all": {}}

        results = es_client.search(
            index=ESIndex.CLUSTERING_RESULT,
            query=query,
            size=size
        )

        cluster_stats = {}
        for r in results:
            cid = r.get("cluster_id", -1)
            if cid not in cluster_stats:
                cluster_stats[cid] = 0
            cluster_stats[cid] += 1

        return {
            "total": len(results),
            "cluster_stats": cluster_stats,
            "results": results
        }
    except Exception as e:
        logger.error(f"Error fetching results: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/health", response_model=HealthResponse)
async def health_check():
    """헬스체크"""
    es_healthy = es_client.health_check()

    return HealthResponse(
        status="healthy" if es_healthy else "degraded",
        service="clustering",
        dependencies={
            "elasticsearch": es_healthy
        }
    )
