"""Mini-Batch K-Means 클러스터링 모듈"""
import logging
from typing import Optional, List, Dict, Any, Tuple
import numpy as np
from sklearn.cluster import MiniBatchKMeans
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import silhouette_score, calinski_harabasz_score

from .umap_reducer import UMAPReducer

logger = logging.getLogger(__name__)


class MiniBatchKMeansClusterer:
    """Mini-Batch K-Means 기반 대용량 데이터 클러스터링"""

    def __init__(
        self,
        n_clusters: int = 8,
        batch_size: int = 1024,
        max_iter: int = 100,
        n_init: int = 10,
        init: str = "k-means++",
        random_state: int = 42,
        reassignment_ratio: float = 0.01
    ):
        """
        Mini-Batch K-Means 클러스터러 초기화

        Args:
            n_clusters: 클러스터 수
            batch_size: 미니 배치 크기
            max_iter: 최대 반복 횟수
            n_init: 다른 초기화로 실행할 횟수
            init: 초기화 방법 (k-means++/random)
            random_state: 재현성을 위한 시드
            reassignment_ratio: 재할당 비율
        """
        self.n_clusters = n_clusters
        self.batch_size = batch_size
        self.max_iter = max_iter
        self.n_init = n_init
        self.init = init
        self.random_state = random_state
        self.reassignment_ratio = reassignment_ratio

        self.clusterer: Optional[MiniBatchKMeans] = None
        self.scaler = StandardScaler()
        self.labels_: Optional[np.ndarray] = None
        self.cluster_centers_: Optional[np.ndarray] = None
        self.inertia_: Optional[float] = None
        self.umap_coords_: Optional[np.ndarray] = None
        self.silhouette_: Optional[float] = None
        self.calinski_harabasz_: Optional[float] = None

    def fit(self, X: np.ndarray) -> "MiniBatchKMeansClusterer":
        """
        Mini-Batch K-Means 클러스터링 학습

        Args:
            X: 입력 데이터 (n_samples, n_features)

        Returns:
            self
        """
        logger.info(f"Starting Mini-Batch K-Means with {X.shape[0]} samples, {self.n_clusters} clusters")

        # 데이터 정규화
        X_scaled = self.scaler.fit_transform(X)

        # 시각화용 2D UMAP 좌표 생성
        if X.shape[0] > 2:
            umap_reducer = UMAPReducer(
                n_components=2,
                n_neighbors=min(15, X.shape[0] - 1)
            )
            self.umap_coords_ = umap_reducer.fit_transform(X_scaled)
        else:
            self.umap_coords_ = np.zeros((X.shape[0], 2))

        # Mini-Batch K-Means 클러스터링
        actual_n_clusters = min(self.n_clusters, X.shape[0])
        self.clusterer = MiniBatchKMeans(
            n_clusters=actual_n_clusters,
            batch_size=min(self.batch_size, X.shape[0]),
            max_iter=self.max_iter,
            n_init=self.n_init,
            init=self.init,
            random_state=self.random_state,
            reassignment_ratio=self.reassignment_ratio
        )

        self.labels_ = self.clusterer.fit_predict(X_scaled)
        self.cluster_centers_ = self.clusterer.cluster_centers_
        self.inertia_ = self.clusterer.inertia_

        # 평가 지표 계산 (클러스터가 2개 이상이고 샘플이 충분할 때)
        if actual_n_clusters >= 2 and X.shape[0] > actual_n_clusters:
            try:
                self.silhouette_ = silhouette_score(X_scaled, self.labels_)
                self.calinski_harabasz_ = calinski_harabasz_score(X_scaled, self.labels_)
            except Exception as e:
                logger.warning(f"Could not compute evaluation metrics: {e}")

        n_clusters = len(set(self.labels_))
        logger.info(f"Mini-Batch K-Means completed: {n_clusters} clusters, inertia={self.inertia_:.2f}")

        return self

    def fit_predict(self, X: np.ndarray) -> np.ndarray:
        """클러스터링 학습 및 레이블 반환"""
        self.fit(X)
        return self.labels_

    def predict(self, X: np.ndarray) -> np.ndarray:
        """새 데이터에 대한 클러스터 예측"""
        if self.clusterer is None:
            raise ValueError("Model not fitted. Call fit() first.")

        X_scaled = self.scaler.transform(X)
        return self.clusterer.predict(X_scaled)

    def partial_fit(self, X: np.ndarray) -> "MiniBatchKMeansClusterer":
        """
        점진적 학습 (스트리밍 데이터용)

        Args:
            X: 새 데이터 배치

        Returns:
            self
        """
        X_scaled = self.scaler.fit_transform(X) if self.clusterer is None else self.scaler.transform(X)

        if self.clusterer is None:
            self.clusterer = MiniBatchKMeans(
                n_clusters=self.n_clusters,
                batch_size=self.batch_size,
                random_state=self.random_state
            )

        self.clusterer.partial_fit(X_scaled)
        return self

    def find_optimal_clusters(
        self,
        X: np.ndarray,
        min_clusters: int = 2,
        max_clusters: int = 15,
        metric: str = "silhouette"
    ) -> int:
        """
        최적 클러스터 수 탐색 (Elbow method / Silhouette)

        Args:
            X: 입력 데이터
            min_clusters: 최소 클러스터 수
            max_clusters: 최대 클러스터 수
            metric: 평가 지표 (silhouette/inertia)

        Returns:
            최적 클러스터 수
        """
        X_scaled = self.scaler.fit_transform(X)
        max_clusters = min(max_clusters, X.shape[0] - 1)

        scores = []
        for k in range(min_clusters, max_clusters + 1):
            kmeans = MiniBatchKMeans(
                n_clusters=k,
                batch_size=min(self.batch_size, X.shape[0]),
                n_init=3,
                random_state=self.random_state
            )
            labels = kmeans.fit_predict(X_scaled)

            if metric == "silhouette":
                score = silhouette_score(X_scaled, labels)
                scores.append((k, score))
            else:  # inertia
                scores.append((k, kmeans.inertia_))

        # Silhouette는 최대, Inertia는 최소 (elbow 방식)
        if metric == "silhouette":
            optimal = max(scores, key=lambda x: x[1])
        else:
            # Elbow method: 변화율이 급격히 감소하는 지점
            inertias = [s[1] for s in scores]
            deltas = np.diff(inertias)
            elbow_idx = np.argmin(deltas) + 1  # 변화가 가장 작은 지점
            optimal = scores[elbow_idx]

        logger.info(f"Optimal clusters by {metric}: {optimal[0]} (score={optimal[1]:.4f})")
        return optimal[0]

    def get_cluster_summary(self) -> Dict[str, Any]:
        """클러스터링 요약 정보"""
        if self.labels_ is None:
            raise ValueError("Model not fitted. Call fit() first.")

        unique_labels = set(self.labels_)
        cluster_sizes = {}
        for label in unique_labels:
            cluster_sizes[int(label)] = int(np.sum(self.labels_ == label))

        return {
            "n_clusters": len(unique_labels),
            "n_samples": len(self.labels_),
            "cluster_sizes": cluster_sizes,
            "inertia": float(self.inertia_) if self.inertia_ else None,
            "silhouette_score": float(self.silhouette_) if self.silhouette_ else None,
            "calinski_harabasz_score": float(self.calinski_harabasz_) if self.calinski_harabasz_ else None
        }

    def get_results(self) -> List[Dict[str, Any]]:
        """전체 클러스터링 결과 반환"""
        if self.labels_ is None:
            raise ValueError("Model not fitted. Call fit() first.")

        # 각 샘플과 클러스터 중심 사이의 거리 계산
        if self.clusterer is not None:
            X_scaled = self.scaler.transform(
                self.scaler.inverse_transform(
                    np.zeros((len(self.labels_), self.cluster_centers_.shape[1]))
                )
            )

        results = []
        for i in range(len(self.labels_)):
            result = {
                "cluster_id": int(self.labels_[i]),
            }
            if self.umap_coords_ is not None:
                result["umap_coords"] = self.umap_coords_[i].tolist()
            results.append(result)

        return results

    def get_cluster_centers(self) -> List[List[float]]:
        """클러스터 중심점 반환 (원본 스케일)"""
        if self.cluster_centers_ is None:
            raise ValueError("Model not fitted. Call fit() first.")

        centers_original = self.scaler.inverse_transform(self.cluster_centers_)
        return centers_original.tolist()

    def get_params(self) -> Dict[str, Any]:
        """현재 파라미터 반환"""
        return {
            "n_clusters": self.n_clusters,
            "batch_size": self.batch_size,
            "max_iter": self.max_iter,
            "n_init": self.n_init,
            "init": self.init
        }


def cluster_with_minibatch_kmeans(
    data: np.ndarray,
    n_clusters: int = 8,
    batch_size: int = 1024,
    auto_select: bool = False
) -> Tuple[np.ndarray, np.ndarray]:
    """
    편의 함수: Mini-Batch K-Means 클러스터링 실행

    Args:
        data: 입력 데이터
        n_clusters: 클러스터 수
        batch_size: 미니 배치 크기
        auto_select: 자동 클러스터 수 선택 여부

    Returns:
        (클러스터 레이블, UMAP 좌표)
    """
    clusterer = MiniBatchKMeansClusterer(
        n_clusters=n_clusters,
        batch_size=batch_size
    )

    if auto_select:
        optimal_n = clusterer.find_optimal_clusters(data)
        clusterer = MiniBatchKMeansClusterer(
            n_clusters=optimal_n,
            batch_size=batch_size
        )

    labels = clusterer.fit_predict(data)
    umap_coords = clusterer.umap_coords_ if clusterer.umap_coords_ is not None else np.zeros((len(labels), 2))

    return labels, umap_coords
