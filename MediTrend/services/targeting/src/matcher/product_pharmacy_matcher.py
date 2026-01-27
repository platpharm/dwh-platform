"""의약품-약국 매칭 알고리즘"""

from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
from collections import defaultdict

import numpy as np

from shared.clients.es_client import es_client
from shared.config import ESIndex
from shared.models.schemas import TargetingResult
from .vector_combiner import VectorCombiner


class ProductPharmacyMatcher:
    """의약품-약국 타겟팅 매칭 클래스"""

    def __init__(self):
        self.es_client = es_client
        self.vector_combiner = VectorCombiner()

    def get_clustering_results(
        self,
        entity_type: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """클러스터링 결과 조회

        Args:
            entity_type: "product" 또는 "pharmacy", None이면 전체

        Returns:
            클러스터링 결과 리스트
        """
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
        """랭킹 결과 조회 (상위 N개)

        Args:
            top_n: 상위 N개 상품

        Returns:
            랭킹 결과 리스트
        """
        query = {"match_all": {}}

        response = self.es_client.client.search(
            index=ESIndex.RANKING_RESULT,
            body={
                "query": query,
                "size": top_n,
                "sort": [{"ranking_score": {"order": "desc"}}]
            }
        )

        return [hit["_source"] for hit in response["hits"]["hits"]]

    def build_cluster_mapping(
        self,
        clustering_results: List[Dict[str, Any]]
    ) -> Tuple[Dict[int, List[int]], Dict[int, List[int]]]:
        """클러스터 ID별 엔티티 매핑 구축

        Args:
            clustering_results: 클러스터링 결과 리스트

        Returns:
            (상품 클러스터 매핑, 약국 클러스터 매핑)
        """
        product_clusters: Dict[int, List[int]] = defaultdict(list)
        pharmacy_clusters: Dict[int, List[int]] = defaultdict(list)

        for result in clustering_results:
            entity_type = result.get("entity_type")
            entity_id = result.get("entity_id")
            cluster_id = result.get("cluster_id")

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
        """클러스터 간 친화도 계산

        같은 클러스터 ID를 가진 상품과 약국 간의 친화도가 높음
        클러스터 크기를 고려한 정규화

        Args:
            product_cluster_id: 상품 클러스터 ID
            pharmacy_cluster_id: 약국 클러스터 ID
            product_clusters: 상품 클러스터 매핑
            pharmacy_clusters: 약국 클러스터 매핑

        Returns:
            친화도 점수 (0~1)
        """
        # 기본 친화도: 같은 클러스터 ID이면 높은 점수
        if product_cluster_id == pharmacy_cluster_id:
            base_affinity = 0.8
        else:
            # 클러스터 ID 차이에 따른 감소
            cluster_distance = abs(product_cluster_id - pharmacy_cluster_id)
            base_affinity = max(0.1, 0.5 - (cluster_distance * 0.05))

        # 클러스터 크기 기반 보정 (작은 클러스터일수록 특화됨)
        product_cluster_size = len(product_clusters.get(product_cluster_id, []))
        pharmacy_cluster_size = len(pharmacy_clusters.get(pharmacy_cluster_id, []))

        # 정규화 (크기가 작을수록 보너스)
        size_factor = 1.0
        if product_cluster_size > 0 and pharmacy_cluster_size > 0:
            avg_size = (product_cluster_size + pharmacy_cluster_size) / 2
            size_factor = min(1.2, 1.0 + (10 / (avg_size + 10)))

        return min(1.0, base_affinity * size_factor)

    def get_ranking_score_map(
        self,
        ranking_results: List[Dict[str, Any]]
    ) -> Dict[int, Dict[str, Any]]:
        """랭킹 결과를 상품 ID 기반 맵으로 변환

        Args:
            ranking_results: 랭킹 결과 리스트

        Returns:
            상품 ID -> 랭킹 정보 매핑
        """
        return {
            result["product_id"]: {
                "product_name": result.get("product_name"),
                "ranking_score": result.get("ranking_score", 0),
                "rank": result.get("rank", 999)
            }
            for result in ranking_results
            if result.get("product_name")  # product_name 없으면 제외
        }

    def get_pharmacy_info(
        self,
        pharmacy_ids: List
    ) -> Dict[Any, Dict[str, Any]]:
        """약국 정보 조회 (CDC account 데이터에서 직접)

        Args:
            pharmacy_ids: 약국 ID 리스트

        Returns:
            약국 ID -> 약국 정보 매핑
        """
        pharmacy_info = {}

        if not pharmacy_ids:
            return pharmacy_info

        # Convert to strings for ES query
        pharmacy_ids_str = [str(pid) for pid in pharmacy_ids]

        # First get cluster info
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

        # Get account info from CDC (only cu/bh roles = pharmacies)
        try:
            cdc_results = self.es_client.search(
                index=ESIndex.CDC_ACCOUNT,
                query={
                    "bool": {
                        "must": [
                            {"terms": {"id": pharmacy_ids_str}},
                            {"terms": {"role": ["CU", "BH"]}}  # 약국만 필터링 (대문자)
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

                # 약국명 우선 (host_name), 없으면 개인명 (name)
                pharmacy_name = result.get("host_name") or result.get("name")
                if not pharmacy_name:
                    continue  # 이름 없는 약국 스킵
                pharmacy_info[pharmacy_id] = {
                    "pharmacy_name": pharmacy_name,
                    "pharmacy_address": full_address,
                    "region": result.get("region", ""),
                    "city": result.get("city", ""),
                    "cluster_id": cluster_map.get(str(pharmacy_id), 0)
                }
        except Exception as e:
            # Fallback to clustering features
            for result in cluster_results:
                pharmacy_id = result.get("entity_id")
                features = result.get("features", {})
                pharmacy_name = features.get("name")
                if not pharmacy_name:
                    continue  # 이름 없는 약국 스킵
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
        """의약품-약국 매칭 실행

        Args:
            top_n_products: 타겟팅할 상위 상품 수
            top_n_pharmacies: 상품당 추천 약국 수

        Returns:
            타겟팅 결과 리스트
        """
        clustering_results = self.get_clustering_results()
        ranking_results = self.get_ranking_results(top_n=top_n_products)
        product_clusters, pharmacy_clusters = self.build_cluster_mapping(
            clustering_results
        )
        ranking_map = self.get_ranking_score_map(ranking_results)

        product_to_cluster: Dict[int, int] = {}
        for result in clustering_results:
            if result.get("entity_type") == "product":
                product_to_cluster[result["entity_id"]] = result.get("cluster_id", 0)

        pharmacy_to_cluster: Dict[int, int] = {}
        for result in clustering_results:
            if result.get("entity_type") == "pharmacy":
                pharmacy_to_cluster[result["entity_id"]] = result.get("cluster_id", 0)

        all_pharmacy_ids = list(pharmacy_to_cluster.keys())
        pharmacy_info = self.get_pharmacy_info(all_pharmacy_ids)

        targeting_results: List[TargetingResult] = []
        timestamp = datetime.now()

        for product_id, product_info in ranking_map.items():
            product_cluster_id = product_to_cluster.get(product_id, 0)

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
                continue  # 상품명 없으면 스킵

            for pharmacy_id, match_score in top_pharmacies:
                info = pharmacy_info.get(pharmacy_id, {})
                pharmacy_name = info.get("pharmacy_name")
                if not pharmacy_name:
                    continue  # 약국명 없으면 스킵

                result = TargetingResult(
                    product_id=product_id,
                    product_name=product_name,
                    pharmacy_id=pharmacy_id,
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
        """타겟팅 결과 저장

        Args:
            results: 타겟팅 결과 리스트

        Returns:
            (성공 건수, 실패 건수)
        """
        if not results:
            return (0, 0)

        documents = [result.model_dump(mode="json") for result in results]

        for doc in documents:
            if isinstance(doc.get("timestamp"), datetime):
                doc["timestamp"] = doc["timestamp"].isoformat()

        success_count, errors = self.es_client.bulk_index(
            index=ESIndex.TARGETING_RESULT,
            documents=documents,
            id_field=None  # 자동 ID 생성
        )

        return (success_count, len(errors) if isinstance(errors, list) else 0)

    def get_targeting_results(
        self,
        product_id: Optional[int] = None,
        pharmacy_id: Optional[int] = None,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """타겟팅 결과 조회

        Args:
            product_id: 특정 상품 ID (선택)
            pharmacy_id: 특정 약국 ID (선택)
            limit: 최대 결과 수

        Returns:
            타겟팅 결과 리스트
        """
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
        """타겟팅 전체 프로세스 실행

        Args:
            top_n_products: 타겟팅할 상위 상품 수
            top_n_pharmacies: 상품당 추천 약국 수

        Returns:
            실행 결과 정보
        """
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
