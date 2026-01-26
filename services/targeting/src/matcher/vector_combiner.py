"""벡터 결합기 - 다양한 점수를 결합하여 최종 매칭 점수 산출"""

from typing import Dict, List, Optional, Tuple
import numpy as np


class VectorCombiner:
    """다양한 특성 벡터를 결합하여 최종 점수 산출"""

    def __init__(
        self,
        cluster_weight: float = 0.5,
        ranking_weight: float = 0.3,
        pattern_weight: float = 0.2
    ):
        """
        Args:
            cluster_weight: 클러스터 친화도 가중치
            ranking_weight: 랭킹 점수 가중치
            pattern_weight: 구매 패턴 가중치
        """
        self.cluster_weight = cluster_weight
        self.ranking_weight = ranking_weight
        self.pattern_weight = pattern_weight

        # 가중치 정규화
        total = self.cluster_weight + self.ranking_weight + self.pattern_weight
        self.cluster_weight /= total
        self.ranking_weight /= total
        self.pattern_weight /= total

    def combine_scores(
        self,
        cluster_affinity: float,
        ranking_weight: float,
        purchase_pattern_score: Optional[float] = None
    ) -> float:
        """점수들을 가중 결합

        Args:
            cluster_affinity: 클러스터 친화도 (0~1)
            ranking_weight: 랭킹 가중치 (0~1)
            purchase_pattern_score: 구매 패턴 점수 (0~1, 선택)

        Returns:
            최종 매칭 점수 (0~1)
        """
        if purchase_pattern_score is None:
            # 구매 패턴 없으면 다른 가중치 재분배
            adjusted_cluster_weight = self.cluster_weight + (self.pattern_weight * 0.6)
            adjusted_ranking_weight = self.ranking_weight + (self.pattern_weight * 0.4)

            score = (
                cluster_affinity * adjusted_cluster_weight +
                ranking_weight * adjusted_ranking_weight
            )
        else:
            score = (
                cluster_affinity * self.cluster_weight +
                ranking_weight * self.ranking_weight +
                purchase_pattern_score * self.pattern_weight
            )

        return min(1.0, max(0.0, score))

    def combine_feature_vectors(
        self,
        vectors: List[np.ndarray],
        weights: Optional[List[float]] = None
    ) -> np.ndarray:
        """특성 벡터들을 가중 결합

        Args:
            vectors: 벡터 리스트
            weights: 가중치 리스트 (선택, 없으면 균등)

        Returns:
            결합된 벡터
        """
        if not vectors:
            return np.array([])

        if weights is None:
            weights = [1.0 / len(vectors)] * len(vectors)
        else:
            # 정규화
            total = sum(weights)
            weights = [w / total for w in weights]

        # 벡터 차원 맞추기
        max_dim = max(v.shape[0] for v in vectors)
        padded_vectors = []

        for v in vectors:
            if v.shape[0] < max_dim:
                padded = np.zeros(max_dim)
                padded[:v.shape[0]] = v
                padded_vectors.append(padded)
            else:
                padded_vectors.append(v)

        # 가중 합
        result = np.zeros(max_dim)
        for v, w in zip(padded_vectors, weights):
            result += v * w

        return result

    def cosine_similarity(
        self,
        vec1: np.ndarray,
        vec2: np.ndarray
    ) -> float:
        """코사인 유사도 계산

        Args:
            vec1: 벡터 1
            vec2: 벡터 2

        Returns:
            코사인 유사도 (-1~1)
        """
        norm1 = np.linalg.norm(vec1)
        norm2 = np.linalg.norm(vec2)

        if norm1 == 0 or norm2 == 0:
            return 0.0

        return float(np.dot(vec1, vec2) / (norm1 * norm2))

    def euclidean_distance(
        self,
        vec1: np.ndarray,
        vec2: np.ndarray
    ) -> float:
        """유클리드 거리 계산

        Args:
            vec1: 벡터 1
            vec2: 벡터 2

        Returns:
            유클리드 거리
        """
        return float(np.linalg.norm(vec1 - vec2))

    def normalize_vector(
        self,
        vec: np.ndarray
    ) -> np.ndarray:
        """벡터 정규화 (L2 norm)

        Args:
            vec: 입력 벡터

        Returns:
            정규화된 벡터
        """
        norm = np.linalg.norm(vec)
        if norm == 0:
            return vec
        return vec / norm

    def create_target_vector(
        self,
        product_features: Dict[str, float],
        pharmacy_features: Dict[str, float],
        ranking_score: float,
        cluster_affinity: float
    ) -> Dict[str, float]:
        """타겟 벡터 생성

        Args:
            product_features: 상품 특성
            pharmacy_features: 약국 특성
            ranking_score: 랭킹 점수
            cluster_affinity: 클러스터 친화도

        Returns:
            타겟 벡터 (딕셔너리 형태)
        """
        target_vector = {
            "ranking_score": ranking_score / 100,  # 정규화
            "cluster_affinity": cluster_affinity,
        }

        # 상품 특성 추가
        for key, value in product_features.items():
            target_vector[f"product_{key}"] = value

        # 약국 특성 추가
        for key, value in pharmacy_features.items():
            target_vector[f"pharmacy_{key}"] = value

        # 최종 매칭 점수
        target_vector["match_score"] = self.combine_scores(
            cluster_affinity=cluster_affinity,
            ranking_weight=ranking_score / 100
        )

        return target_vector

    def batch_combine_scores(
        self,
        score_tuples: List[Tuple[float, float, Optional[float]]]
    ) -> List[float]:
        """배치로 점수 결합

        Args:
            score_tuples: (cluster_affinity, ranking_weight, purchase_pattern_score) 튜플 리스트

        Returns:
            결합된 점수 리스트
        """
        return [
            self.combine_scores(
                cluster_affinity=t[0],
                ranking_weight=t[1],
                purchase_pattern_score=t[2] if len(t) > 2 else None
            )
            for t in score_tuples
        ]
