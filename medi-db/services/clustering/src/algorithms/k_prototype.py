"""K-Prototype 클러스터링 모듈 (혼합형 데이터용)"""
import logging
from typing import Optional, List, Dict, Any, Tuple
import numpy as np
import pandas as pd
from kmodes.kprototypes import KPrototypes
from sklearn.preprocessing import StandardScaler, LabelEncoder

from .umap_reducer import UMAPReducer

logger = logging.getLogger(__name__)


class KPrototypeClusterer:
    """K-Prototype 기반 혼합형 데이터 클러스터링"""

    def __init__(
        self,
        n_clusters: int = 5,
        init: str = "Huang",
        n_init: int = 10,
        max_iter: int = 100,
        gamma: Optional[float] = None,
        random_state: int = 42
    ):
        """
        K-Prototype 클러스터러 초기화

        Args:
            n_clusters: 클러스터 수
            init: 초기화 방법 (Huang/Cao/random)
            n_init: 다른 초기화로 실행할 횟수
            max_iter: 최대 반복 횟수
            gamma: 범주형 변수 가중치 (None이면 자동)
            random_state: 재현성을 위한 시드
        """
        self.n_clusters = n_clusters
        self.init = init
        self.n_init = n_init
        self.max_iter = max_iter
        self.gamma = gamma
        self.random_state = random_state

        self.clusterer: Optional[KPrototypes] = None
        self.scaler = StandardScaler()
        self.label_encoders: Dict[int, LabelEncoder] = {}
        self.labels_: Optional[np.ndarray] = None
        self.cost_: Optional[float] = None
        self.umap_coords_: Optional[np.ndarray] = None
        self.categorical_indices_: Optional[List[int]] = None

    def _preprocess_data(
        self,
        X: np.ndarray,
        categorical_indices: List[int]
    ) -> np.ndarray:
        """
        데이터 전처리 (수치형 정규화, 범주형 인코딩)

        Args:
            X: 입력 데이터
            categorical_indices: 범주형 변수 인덱스 리스트

        Returns:
            전처리된 데이터
        """
        X_processed = X.copy()
        numerical_indices = [i for i in range(X.shape[1]) if i not in categorical_indices]

        if numerical_indices:
            X_numerical = X[:, numerical_indices].astype(float)
            # NaN/Inf를 0으로 대체하여 StandardScaler 오류 방지
            X_numerical = np.nan_to_num(X_numerical, nan=0.0, posinf=0.0, neginf=0.0)
            X_numerical_scaled = self.scaler.fit_transform(X_numerical)
            for i, idx in enumerate(numerical_indices):
                X_processed[:, idx] = X_numerical_scaled[:, i]

        for idx in categorical_indices:
            le = LabelEncoder()
            X_processed[:, idx] = le.fit_transform(X[:, idx].astype(str))
            self.label_encoders[idx] = le

        return X_processed

    def fit(
        self,
        X: np.ndarray,
        categorical_indices: Optional[List[int]] = None
    ) -> "KPrototypeClusterer":
        """
        K-Prototype 클러스터링 학습

        Args:
            X: 입력 데이터 (n_samples, n_features)
            categorical_indices: 범주형 변수 인덱스 (없으면 자동 감지)

        Returns:
            self
        """
        if X.shape[0] == 0:
            raise ValueError("Cannot cluster empty dataset.")
        if X.shape[0] == 1:
            self.labels_ = np.array([0])
            self.cost_ = 0.0
            self.umap_coords_ = np.zeros((1, 2))
            self.categorical_indices_ = categorical_indices or []
            logger.warning("Only 1 sample provided; assigned to cluster 0.")
            return self

        logger.info(f"Starting K-Prototype clustering with {X.shape[0]} samples")

        if categorical_indices is None:
            categorical_indices = self._detect_categorical(X)

        self.categorical_indices_ = categorical_indices
        logger.info(f"Categorical indices: {categorical_indices}")

        X_processed = self._preprocess_data(X, categorical_indices)


        numerical_indices = [i for i in range(X.shape[1]) if i not in categorical_indices]
        if numerical_indices:
            X_numerical = X_processed[:, numerical_indices].astype(float)
            if X_numerical.shape[0] > 2:
                umap_reducer = UMAPReducer(
                    n_components=2,
                    n_neighbors=min(15, X_numerical.shape[0] - 1)
                )
                self.umap_coords_ = umap_reducer.fit_transform(X_numerical)
            else:
                self.umap_coords_ = np.zeros((X.shape[0], 2))
        else:
            self.umap_coords_ = np.zeros((X.shape[0], 2))

        self.clusterer = KPrototypes(
            n_clusters=min(self.n_clusters, X.shape[0]),
            init=self.init,
            n_init=self.n_init,
            max_iter=self.max_iter,
            gamma=self.gamma,
            random_state=self.random_state,
            verbose=0
        )

        self.labels_ = self.clusterer.fit_predict(X_processed, categorical=categorical_indices)
        self.cost_ = self.clusterer.cost_

        n_clusters = len(set(self.labels_))
        logger.info(f"K-Prototype completed: {n_clusters} clusters, cost={self.cost_:.2f}")

        return self

    def _detect_categorical(self, X: np.ndarray) -> List[int]:
        """범주형 변수 자동 감지"""
        categorical_indices = []
        for i in range(X.shape[1]):
            col = X[:, i]
            # 문자열이거나 고유값이 적으면 범주형으로 판단
            try:
                col_float = col.astype(float)
                unique_ratio = len(np.unique(col_float)) / len(col_float)
                if unique_ratio < 0.05:  # 고유값 비율이 5% 미만이면 범주형
                    categorical_indices.append(i)
            except (ValueError, TypeError):
                categorical_indices.append(i)

        return categorical_indices

    def fit_predict(
        self,
        X: np.ndarray,
        categorical_indices: Optional[List[int]] = None
    ) -> np.ndarray:
        """클러스터링 학습 및 레이블 반환"""
        self.fit(X, categorical_indices)
        return self.labels_

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
            "cost": float(self.cost_) if self.cost_ else None,
            "categorical_indices": self.categorical_indices_
        }

    def get_results(self) -> List[Dict[str, Any]]:
        """전체 클러스터링 결과 반환"""
        if self.labels_ is None:
            raise ValueError("Model not fitted. Call fit() first.")

        results = []
        for i in range(len(self.labels_)):
            result = {
                "cluster_id": int(self.labels_[i]),
            }
            if self.umap_coords_ is not None:
                result["umap_coords"] = self.umap_coords_[i].tolist()
            results.append(result)

        return results

    def get_params(self) -> Dict[str, Any]:
        """현재 파라미터 반환"""
        return {
            "n_clusters": self.n_clusters,
            "init": self.init,
            "n_init": self.n_init,
            "max_iter": self.max_iter,
            "gamma": self.gamma
        }


def cluster_with_kprototype(
    data: np.ndarray,
    n_clusters: int = 5,
    categorical_indices: Optional[List[int]] = None
) -> Tuple[np.ndarray, np.ndarray]:
    """
    편의 함수: K-Prototype 클러스터링 실행

    Args:
        data: 입력 데이터
        n_clusters: 클러스터 수
        categorical_indices: 범주형 변수 인덱스

    Returns:
        (클러스터 레이블, UMAP 좌표)
    """
    clusterer = KPrototypeClusterer(n_clusters=n_clusters)
    labels = clusterer.fit_predict(data, categorical_indices)
    umap_coords = clusterer.umap_coords_ if clusterer.umap_coords_ is not None else np.zeros((len(labels), 2))

    return labels, umap_coords
