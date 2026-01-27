"""Gaussian Mixture Model (GMM) 클러스터링 모듈"""
import logging
from typing import Optional, List, Dict, Any, Tuple
import numpy as np
from sklearn.mixture import GaussianMixture
from sklearn.preprocessing import StandardScaler

from .umap_reducer import UMAPReducer

logger = logging.getLogger(__name__)


class GMMClusterer:
    """Gaussian Mixture Model 기반 확률적 클러스터링"""

    def __init__(
        self,
        n_components: int = 5,
        covariance_type: str = "full",
        max_iter: int = 100,
        n_init: int = 10,
        init_params: str = "kmeans",
        random_state: int = 42,
        reg_covar: float = 1e-6
    ):
        """
        GMM 클러스터러 초기화

        Args:
            n_components: 가우시안 컴포넌트(클러스터) 수
            covariance_type: 공분산 타입 (full/tied/diag/spherical)
            max_iter: 최대 반복 횟수
            n_init: 다른 초기화로 실행할 횟수
            init_params: 파라미터 초기화 방법 (kmeans/random)
            random_state: 재현성을 위한 시드
            reg_covar: 공분산 정규화 값
        """
        self.n_components = n_components
        self.covariance_type = covariance_type
        self.max_iter = max_iter
        self.n_init = n_init
        self.init_params = init_params
        self.random_state = random_state
        self.reg_covar = reg_covar

        self.clusterer: Optional[GaussianMixture] = None
        self.scaler = StandardScaler()
        self.labels_: Optional[np.ndarray] = None
        self.probabilities_: Optional[np.ndarray] = None
        self.umap_coords_: Optional[np.ndarray] = None
        self.bic_: Optional[float] = None
        self.aic_: Optional[float] = None

    def fit(self, X: np.ndarray) -> "GMMClusterer":
        """
        GMM 클러스터링 학습

        Args:
            X: 입력 데이터 (n_samples, n_features)

        Returns:
            self
        """
        logger.info(f"Starting GMM clustering with {X.shape[0]} samples, {self.n_components} components")

        X_scaled = self.scaler.fit_transform(X)


        if X.shape[0] > 2:
            umap_reducer = UMAPReducer(
                n_components=2,
                n_neighbors=min(15, X.shape[0] - 1)
            )
            self.umap_coords_ = umap_reducer.fit_transform(X_scaled)
        else:
            self.umap_coords_ = np.zeros((X.shape[0], 2))

        self.clusterer = GaussianMixture(
            n_components=min(self.n_components, X.shape[0]),
            covariance_type=self.covariance_type,
            max_iter=self.max_iter,
            n_init=self.n_init,
            init_params=self.init_params,
            random_state=self.random_state,
            reg_covar=self.reg_covar
        )

        self.clusterer.fit(X_scaled)
        self.labels_ = self.clusterer.predict(X_scaled)
        self.probabilities_ = self.clusterer.predict_proba(X_scaled)

        self.bic_ = self.clusterer.bic(X_scaled)
        self.aic_ = self.clusterer.aic(X_scaled)

        n_clusters = len(set(self.labels_))
        logger.info(f"GMM completed: {n_clusters} clusters, BIC={self.bic_:.2f}, AIC={self.aic_:.2f}")

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

    def predict_proba(self, X: np.ndarray) -> np.ndarray:
        """새 데이터에 대한 클러스터 확률 예측"""
        if self.clusterer is None:
            raise ValueError("Model not fitted. Call fit() first.")

        X_scaled = self.scaler.transform(X)
        return self.clusterer.predict_proba(X_scaled)

    def find_optimal_components(
        self,
        X: np.ndarray,
        min_components: int = 2,
        max_components: int = 15,
        criterion: str = "bic"
    ) -> int:
        """
        BIC/AIC 기반 최적 컴포넌트 수 탐색

        Args:
            X: 입력 데이터
            min_components: 최소 컴포넌트 수
            max_components: 최대 컴포넌트 수
            criterion: 평가 기준 (bic/aic)

        Returns:
            최적 컴포넌트 수
        """
        X_scaled = self.scaler.fit_transform(X)
        max_components = min(max_components, X.shape[0] - 1)

        scores = []
        for n in range(min_components, max_components + 1):
            gmm = GaussianMixture(
                n_components=n,
                covariance_type=self.covariance_type,
                max_iter=self.max_iter,
                n_init=3,
                random_state=self.random_state
            )
            gmm.fit(X_scaled)

            if criterion == "bic":
                scores.append((n, gmm.bic(X_scaled)))
            else:
                scores.append((n, gmm.aic(X_scaled)))

        optimal = min(scores, key=lambda x: x[1])
        logger.info(f"Optimal components by {criterion}: {optimal[0]} (score={optimal[1]:.2f})")

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
            "bic": float(self.bic_) if self.bic_ else None,
            "aic": float(self.aic_) if self.aic_ else None,
            "converged": self.clusterer.converged_ if self.clusterer else None
        }

    def get_results(self) -> List[Dict[str, Any]]:
        """전체 클러스터링 결과 반환"""
        if self.labels_ is None:
            raise ValueError("Model not fitted. Call fit() first.")

        results = []
        for i in range(len(self.labels_)):
            result = {
                "cluster_id": int(self.labels_[i]),
                "probability": float(np.max(self.probabilities_[i])) if self.probabilities_ is not None else 1.0,
                "cluster_probabilities": self.probabilities_[i].tolist() if self.probabilities_ is not None else None
            }
            if self.umap_coords_ is not None:
                result["umap_coords"] = self.umap_coords_[i].tolist()
            results.append(result)

        return results

    def get_params(self) -> Dict[str, Any]:
        """현재 파라미터 반환"""
        return {
            "n_components": self.n_components,
            "covariance_type": self.covariance_type,
            "max_iter": self.max_iter,
            "n_init": self.n_init,
            "init_params": self.init_params
        }


def cluster_with_gmm(
    data: np.ndarray,
    n_components: int = 5,
    covariance_type: str = "full",
    auto_select: bool = False
) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
    """
    편의 함수: GMM 클러스터링 실행

    Args:
        data: 입력 데이터
        n_components: 컴포넌트 수
        covariance_type: 공분산 타입
        auto_select: 자동 컴포넌트 수 선택 여부

    Returns:
        (클러스터 레이블, 확률, UMAP 좌표)
    """
    clusterer = GMMClusterer(
        n_components=n_components,
        covariance_type=covariance_type
    )

    if auto_select:
        optimal_n = clusterer.find_optimal_components(data)
        clusterer = GMMClusterer(
            n_components=optimal_n,
            covariance_type=covariance_type
        )

    labels = clusterer.fit_predict(data)
    probs = clusterer.probabilities_ if clusterer.probabilities_ is not None else np.ones((len(labels), 1))
    umap_coords = clusterer.umap_coords_ if clusterer.umap_coords_ is not None else np.zeros((len(labels), 2))

    return labels, probs, umap_coords
