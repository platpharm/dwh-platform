
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
from collections import defaultdict

import numpy as np

from shared.clients.es_client import es_client
from shared.config import ESIndex
from shared.models.schemas import TargetingResult
from .vector_combiner import VectorCombiner

class ProductPharmacyMatcher:

    def __init__(self):
        self.es_client = es_client
        self.vector_combiner = VectorCombiner()

    def get_clustering_results(
        self,
        entity_type: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        if entity_type:
            query = {"term": {"entity_type": entity_type}}
        else:
            query = {"match_all": {}}

        results = []
        for doc in self.es_client.scroll_search(
            index=ESIndex.CLUSTERING_RESULT,
            query=query,
            size=1000
        ):
            results.append(doc)

        return results

    def get_ranking_results(
        self,
        top_n: int = 100
    ) -> List[Dict[str, Any]]:
        query = {"match_all": {}}

        response = self.es_client.client.search(
            index=ESIndex.RANKING_RESULT,
            body={
                "query": query,
                "size": top_n,
                "sort": [{"popularity_score": {"order": "desc"}}]
            }
        )

        return [hit["_source"] for hit in response["hits"]["hits"]]

    def build_cluster_mapping(
        self,
        clustering_results: List[Dict[str, Any]]
    ) -> Tuple[Dict[int, List], Dict[int, List]]:
        product_clusters: Dict[int, List] = defaultdict(list)
        pharmacy_clusters: Dict[int, List] = defaultdict(list)

        for result in clustering_results:
            entity_type = result.get("entity_type")
            entity_id = result.get("entity_id")
            cluster_id = result.get("cluster_id")

            if cluster_id is None or entity_id is None:
                continue

            if entity_type == "product":
                product_clusters[cluster_id].append(entity_id)
            elif entity_type == "pharmacy":
                pharmacy_clusters[cluster_id].append(entity_id)

        return dict(product_clusters), dict(pharmacy_clusters)

    def calculate_cluster_affinity(
        self,
        product_cluster_id: int,
        pharmacy_cluster_id: int,
        product_clusters: Dict[int, List[int]],
        pharmacy_clusters: Dict[int, List[int]]
    ) -> float:
        if product_cluster_id == pharmacy_cluster_id:
            base_affinity = 0.8
        else:
            cluster_distance = abs(product_cluster_id - pharmacy_cluster_id)
            base_affinity = max(0.1, 0.5 - (cluster_distance * 0.05))

        product_cluster_size = len(product_clusters.get(product_cluster_id, []))
        pharmacy_cluster_size = len(pharmacy_clusters.get(pharmacy_cluster_id, []))

        size_factor = 1.0
        if product_cluster_size > 0 and pharmacy_cluster_size > 0:
            avg_size = (product_cluster_size + pharmacy_cluster_size) / 2
            size_factor = min(1.2, max(0.8, 10 / (avg_size + 10) + 0.8))

        return min(1.0, base_affinity * size_factor)

    def get_ranking_score_map(
        self,
        ranking_results: List[Dict[str, Any]]
    ) -> Dict[Any, Dict[str, Any]]:
        return {
            result["product_id"]: {
                "product_name": result.get("product_name"),
                "ranking_score": result.get("popularity_score", 0),
                "rank": result.get("rank", 999)
            }
            for result in ranking_results
            if result.get("product_name")
        }

    def get_pharmacy_info(
        self,
        pharmacy_ids: List
    ) -> Dict[Any, Dict[str, Any]]:
        pharmacy_info = {}

        if not pharmacy_ids:
            return pharmacy_info

        pharmacy_ids_str = [str(pid) for pid in pharmacy_ids]

        cluster_query = {
            "bool": {
                "must": [
                    {"term": {"entity_type": "pharmacy"}},
                    {"terms": {"entity_id": pharmacy_ids_str}}
                ]
            }
        }

        cluster_results = self.es_client.search(
            index=ESIndex.CLUSTERING_RESULT,
            query=cluster_query,
            size=len(pharmacy_ids)
        )

        cluster_map = {}
        for result in cluster_results:
            cluster_map[str(result.get("entity_id"))] = result.get("cluster_id", 0)

        try:
            cdc_results = self.es_client.search(
                index=ESIndex.CDC_ACCOUNT,
                query={
                    "bool": {
                        "must": [
                            {"terms": {"id": pharmacy_ids_str}},
                            {"terms": {"role": ["CU", "BH"]}}
                        ],
                        "must_not": [
                            {"exists": {"field": "deleted_at"}}
                        ]
                    }
                },
                size=len(pharmacy_ids)
            )

            for result in cdc_results:
                pharmacy_id = result.get("id")
                address_parts = [
                    result.get("address1", ""),
                    result.get("address2", ""),
                    result.get("address3", "")
                ]
                full_address = " ".join(filter(None, address_parts))

                pharmacy_name = result.get("host_name") or result.get("name")
                if not pharmacy_name:
                    continue
                info_dict = {
                    "pharmacy_name": pharmacy_name,
                    "pharmacy_address": full_address,
                    "region": result.get("region", ""),
                    "city": result.get("city", ""),
                    "cluster_id": cluster_map.get(str(pharmacy_id), 0)
                }
                pharmacy_info[pharmacy_id] = info_dict
                try:
                    alt_key = int(pharmacy_id) if isinstance(pharmacy_id, str) else str(pharmacy_id)
                    pharmacy_info[alt_key] = info_dict
                except (ValueError, TypeError):
                    pass
        except Exception as e:
            for result in cluster_results:
                pharmacy_id = result.get("entity_id")
                features = result.get("features", {})
                pharmacy_name = features.get("name")
                if not pharmacy_name:
                    continue
                pharmacy_info[pharmacy_id] = {
                    "pharmacy_name": pharmacy_name,
                    "pharmacy_address": features.get("address", ""),
                    "cluster_id": result.get("cluster_id", 0)
                }

        return pharmacy_info

    def match_products_to_pharmacies(
        self,
        top_n_products: int = 100,
        top_n_pharmacies: int = 50
    ) -> List[TargetingResult]:
        clustering_results = self.get_clustering_results()
        ranking_results = self.get_ranking_results(top_n=top_n_products)
        product_clusters, pharmacy_clusters = self.build_cluster_mapping(
            clustering_results
        )
        ranking_map = self.get_ranking_score_map(ranking_results)

        if not clustering_results:
            return []

        product_to_cluster: Dict[Any, int] = {}
        for result in clustering_results:
            if result.get("entity_type") == "product":
                entity_id = result.get("entity_id")
                if entity_id is not None:
                    product_to_cluster[entity_id] = result.get("cluster_id", 0)

        pharmacy_to_cluster: Dict[Any, int] = {}
        for result in clustering_results:
            if result.get("entity_type") == "pharmacy":
                entity_id = result.get("entity_id")
                if entity_id is not None:
                    pharmacy_to_cluster[entity_id] = result.get("cluster_id", 0)

        all_pharmacy_ids = list(pharmacy_to_cluster.keys())
        pharmacy_info = self.get_pharmacy_info(all_pharmacy_ids)

        targeting_results: List[TargetingResult] = []
        timestamp = datetime.now()

        for product_id, product_info in ranking_map.items():
            product_cluster_id = product_to_cluster.get(product_id)
            if product_cluster_id is None:
                product_cluster_id = product_to_cluster.get(str(product_id), 0)

            pharmacy_scores: List[Tuple[int, float]] = []

            for pharmacy_id, pharmacy_cluster_id in pharmacy_to_cluster.items():
                cluster_affinity = self.calculate_cluster_affinity(
                    product_cluster_id,
                    pharmacy_cluster_id,
                    product_clusters,
                    pharmacy_clusters
                )

                ranking_weight = product_info.get("ranking_score", 50) / 100

                match_score = self.vector_combiner.combine_scores(
                    cluster_affinity=cluster_affinity,
                    ranking_weight=ranking_weight
                )

                pharmacy_scores.append((pharmacy_id, match_score))

            pharmacy_scores.sort(key=lambda x: x[1], reverse=True)
            top_pharmacies = pharmacy_scores[:top_n_pharmacies]

            product_name = product_info.get("product_name")
            if not product_name:
                continue

            for pharmacy_id, match_score in top_pharmacies:
                info = pharmacy_info.get(pharmacy_id) or pharmacy_info.get(str(pharmacy_id)) or {}
                pharmacy_name = info.get("pharmacy_name")
                if not pharmacy_name:
                    continue

                try:
                    safe_product_id = int(product_id)
                    safe_pharmacy_id = int(pharmacy_id)
                except (ValueError, TypeError):
                    continue

                result = TargetingResult(
                    product_id=safe_product_id,
                    product_name=product_name,
                    pharmacy_id=safe_pharmacy_id,
                    pharmacy_name=pharmacy_name,
                    pharmacy_address=info.get("pharmacy_address"),
                    match_score=round(match_score, 4),
                    product_cluster_id=product_cluster_id,
                    pharmacy_cluster_id=info.get("cluster_id", 0),
                    ranking_score=product_info.get("ranking_score", 0),
                    timestamp=timestamp
                )

                targeting_results.append(result)

        return targeting_results

    def save_targeting_results(
        self,
        results: List[TargetingResult]
    ) -> Tuple[int, int]:
        if not results:
            return (0, 0)

        documents = [result.model_dump(mode="json") for result in results]

        for doc in documents:
            if isinstance(doc.get("timestamp"), datetime):
                doc["timestamp"] = doc["timestamp"].isoformat()

        try:
            success_count, errors = self.es_client.bulk_index(
                index=ESIndex.TARGETING_RESULT,
                documents=documents,
                id_field=None
            )

            return (success_count, len(errors) if isinstance(errors, list) else 0)
        except Exception as e:
            import logging
            logging.getLogger(__name__).error(f"Bulk index error: {e}")
            return (0, len(documents))

    def get_targeting_results(
        self,
        product_id: Optional[int] = None,
        pharmacy_id: Optional[int] = None,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        must_conditions = []

        if product_id is not None:
            must_conditions.append({"term": {"product_id": product_id}})

        if pharmacy_id is not None:
            must_conditions.append({"term": {"pharmacy_id": pharmacy_id}})

        if must_conditions:
            query = {"bool": {"must": must_conditions}}
        else:
            query = {"match_all": {}}

        response = self.es_client.client.search(
            index=ESIndex.TARGETING_RESULT,
            body={
                "query": query,
                "size": limit,
                "sort": [{"match_score": {"order": "desc"}}]
            }
        )

        return [hit["_source"] for hit in response["hits"]["hits"]]

    def run_targeting(
        self,
        top_n_products: int = 100,
        top_n_pharmacies: int = 50
    ) -> Dict[str, Any]:
        results = self.match_products_to_pharmacies(
            top_n_products=top_n_products,
            top_n_pharmacies=top_n_pharmacies
        )

        success_count, error_count = self.save_targeting_results(results)

        return {
            "total_matches": len(results),
            "saved_count": success_count,
            "error_count": error_count,
            "top_n_products": top_n_products,
            "top_n_pharmacies": top_n_pharmacies
        }

product_pharmacy_matcher = ProductPharmacyMatcher()
