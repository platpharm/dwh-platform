"""HDBSCAN 클러스터링 모듈"""
import logging
from typing import Optional, List, Dict, Any, Tuple
import numpy as np
import hdbscan
from sklearn.preprocessing import StandardScaler

from .umap_reducer import UMAPReducer

logger = logging.getLogger(__name__)


class HDBSCANClusterer:
    """HDBSCAN 기반 밀도 클러스터링"""

    def __init__(
        self,
        min_cluster_size: int = 5,
        min_samples: Optional[int] = None,
        metric: str = "euclidean",
        cluster_selection_method: str = "eom",
        use_umap: bool = True,
        umap_n_components: int = 10,
        umap_n_neighbors: int = 15
    ):
        """
        HDBSCAN 클러스터러 초기화

        Args:
            min_cluster_size: 최소 클러스터 크기
            min_samples: 코어 포인트 판정 최소 샘플 수
            metric: 거리 측정 방법
            cluster_selection_method: 클러스터 선택 방법 (eom/leaf)
            use_umap: UMAP 전처리 사용 여부
            umap_n_components: UMAP 축소 차원
            umap_n_neighbors: UMAP 이웃 수
        """
        self.min_cluster_size = min_cluster_size
        self.min_samples = min_samples
        self.metric = metric
        self.cluster_selection_method = cluster_selection_method
        self.use_umap = use_umap
        self.umap_n_components = umap_n_components
        self.umap_n_neighbors = umap_n_neighbors

        self.clusterer: Optional[hdbscan.HDBSCAN] = None
        self.scaler = StandardScaler()
        self.umap_reducer: Optional[UMAPReducer] = None
        self.labels_: Optional[np.ndarray] = None
        self.probabilities_: Optional[np.ndarray] = None
        self.umap_coords_: Optional[np.ndarray] = None

    def fit(self, X: np.ndarray) -> "HDBSCANClusterer":
        """
        데이터에 HDBSCAN 클러스터링 학습

        Args:
            X: 입력 데이터 (n_samples, n_features)

        Returns:
            self
        """
        logger.info(f"Starting HDBSCAN clustering with {X.shape[0]} samples")

        X_scaled = self.scaler.fit_transform(X)


        if self.use_umap and X.shape[1] > self.umap_n_components:
            logger.info(f"Applying UMAP: {X.shape[1]} -> {self.umap_n_components} dimensions")
            self.umap_reducer = UMAPReducer(
                n_components=self.umap_n_components,
                n_neighbors=min(self.umap_n_neighbors, X.shape[0] - 1)
            )
            X_reduced = self.umap_reducer.fit_transform(X_scaled)
        else:
            X_reduced = X_scaled

        umap_2d = UMAPReducer(n_components=2, n_neighbors=min(15, X.shape[0] - 1))
        self.umap_coords_ = umap_2d.fit_transform(X_scaled)

        self.clusterer = hdbscan.HDBSCAN(
            min_cluster_size=self.min_cluster_size,
            min_samples=self.min_samples,
            metric=self.metric,
            cluster_selection_method=self.cluster_selection_method,
            prediction_data=True
        )
        self.clusterer.fit(X_reduced)

        self.labels_ = self.clusterer.labels_
        self.probabilities_ = self.clusterer.probabilities_

        n_clusters = len(set(self.labels_)) - (1 if -1 in self.labels_ else 0)
        n_noise = list(self.labels_).count(-1)

        logger.info(f"HDBSCAN completed: {n_clusters} clusters, {n_noise} noise points")

        return self

    def fit_predict(self, X: np.ndarray) -> np.ndarray:
        """
        클러스터링 학습 및 레이블 반환

        Args:
            X: 입력 데이터

        Returns:
            클러스터 레이블 배열
        """
        self.fit(X)
        return self.labels_

    def get_cluster_summary(self) -> Dict[str, Any]:
        """클러스터링 요약 정보"""
        if self.labels_ is None:
            raise ValueError("Model not fitted. Call fit() first.")

        unique_labels = set(self.labels_)
        n_clusters = len(unique_labels) - (1 if -1 in unique_labels else 0)
        n_noise = list(self.labels_).count(-1)

        cluster_sizes = {}
        for label in unique_labels:
            if label != -1:
                cluster_sizes[int(label)] = int(np.sum(self.labels_ == label))

        return {
            "n_clusters": n_clusters,
            "n_noise": n_noise,
            "n_samples": len(self.labels_),
            "cluster_sizes": cluster_sizes,
            "noise_ratio": n_noise / len(self.labels_) if len(self.labels_) > 0 else 0
        }

    def get_results(self) -> List[Dict[str, Any]]:
        """
        전체 클러스터링 결과 반환

        Returns:
            각 샘플의 클러스터 정보 리스트
        """
        if self.labels_ is None:
            raise ValueError("Model not fitted. Call fit() first.")

        results = []
        for i in range(len(self.labels_)):
            result = {
                "cluster_id": int(self.labels_[i]),
                "probability": float(self.probabilities_[i]) if self.probabilities_ is not None else 1.0,
            }
            if self.umap_coords_ is not None:
                result["umap_coords"] = self.umap_coords_[i].tolist()
            results.append(result)

        return results

    def get_params(self) -> Dict[str, Any]:
        """현재 파라미터 반환"""
        return {
            "min_cluster_size": self.min_cluster_size,
            "min_samples": self.min_samples,
            "metric": self.metric,
            "cluster_selection_method": self.cluster_selection_method,
            "use_umap": self.use_umap,
            "umap_n_components": self.umap_n_components
        }


def cluster_with_hdbscan(
    data: np.ndarray,
    min_cluster_size: int = 5,
    min_samples: Optional[int] = None,
    use_umap: bool = True
) -> Tuple[np.ndarray, np.ndarray]:
    """
    편의 함수: HDBSCAN 클러스터링 실행

    Args:
        data: 입력 데이터
        min_cluster_size: 최소 클러스터 크기
        min_samples: 코어 포인트 최소 샘플
        use_umap: UMAP 전처리 사용

    Returns:
        (클러스터 레이블, UMAP 좌표)
    """
    clusterer = HDBSCANClusterer(
        min_cluster_size=min_cluster_size,
        min_samples=min_samples,
        use_umap=use_umap
    )
    labels = clusterer.fit_predict(data)
    umap_coords = clusterer.umap_coords_ if clusterer.umap_coords_ is not None else np.zeros((len(labels), 2))

    return labels, umap_coords
