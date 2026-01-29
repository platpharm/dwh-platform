
from typing import Dict, List, Optional, Tuple
import numpy as np

class VectorCombiner:

    def __init__(
        self,
        cluster_weight: float = 0.5,
        ranking_weight: float = 0.3,
        pattern_weight: float = 0.2
    ):
        self.cluster_weight = cluster_weight
        self.ranking_weight = ranking_weight
        self.pattern_weight = pattern_weight

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
        if purchase_pattern_score is None:
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
        if not vectors:
            return np.array([])

        if weights is None:
            weights = [1.0 / len(vectors)] * len(vectors)
        else:
            total = sum(weights)
            weights = [w / total for w in weights]

        max_dim = max(v.shape[0] for v in vectors)
        padded_vectors = []

        for v in vectors:
            if v.shape[0] < max_dim:
                padded = np.zeros(max_dim)
                padded[:v.shape[0]] = v
                padded_vectors.append(padded)
            else:
                padded_vectors.append(v)

        result = np.zeros(max_dim)
        for v, w in zip(padded_vectors, weights):
            result += v * w

        return result

    def cosine_similarity(
        self,
        vec1: np.ndarray,
        vec2: np.ndarray
    ) -> float:
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
        return float(np.linalg.norm(vec1 - vec2))

    def normalize_vector(
        self,
        vec: np.ndarray
    ) -> np.ndarray:
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
        target_vector = {
            "ranking_score": ranking_score / 100,
            "cluster_affinity": cluster_affinity,
        }

        for key, value in product_features.items():
            target_vector[f"product_{key}"] = value

        for key, value in pharmacy_features.items():
            target_vector[f"pharmacy_{key}"] = value

        target_vector["match_score"] = self.combine_scores(
            cluster_affinity=cluster_affinity,
            ranking_weight=ranking_score / 100
        )

        return target_vector

    def batch_combine_scores(
        self,
        score_tuples: List[Tuple[float, float, Optional[float]]]
    ) -> List[float]:
        return [
            self.combine_scores(
                cluster_affinity=t[0],
                ranking_weight=t[1],
                purchase_pattern_score=t[2] if len(t) > 2 else None
            )
            for t in score_tuples
        ]
