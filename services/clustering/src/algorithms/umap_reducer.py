"""UMAP 차원 축소 모듈"""
import logging
from typing import Optional, List, Dict, Any
import numpy as np
import umap

logger = logging.getLogger(__name__)


class UMAPReducer:
    """UMAP 기반 차원 축소"""

    def __init__(
        self,
        n_components: int = 2,
        n_neighbors: int = 15,
        min_dist: float = 0.1,
        metric: str = "euclidean",
        random_state: int = 42
    ):
        """
        UMAP 차원 축소기 초기화

        Args:
            n_components: 축소할 차원 수 (시각화용 2D 기본값)
            n_neighbors: 이웃 수 (더 크면 전역 구조 보존)
            min_dist: 최소 거리 (더 작으면 밀집된 클러스터)
            metric: 거리 측정 방법
            random_state: 재현성을 위한 시드
        """
        self.n_components = n_components
        self.n_neighbors = n_neighbors
        self.min_dist = min_dist
        self.metric = metric
        self.random_state = random_state
        self.reducer: Optional[umap.UMAP] = None
        self.embedding_: Optional[np.ndarray] = None

    def fit(self, X: np.ndarray) -> "UMAPReducer":
        """
        데이터에 UMAP 모델 학습

        Args:
            X: 입력 데이터 (n_samples, n_features)

        Returns:
            self
        """
        logger.info(f"Fitting UMAP with {X.shape[0]} samples, {X.shape[1]} features")

        self.reducer = umap.UMAP(
            n_components=self.n_components,
            n_neighbors=min(self.n_neighbors, X.shape[0] - 1),
            min_dist=self.min_dist,
            metric=self.metric,
            random_state=self.random_state,
            verbose=False
        )
        self.reducer.fit(X)

        logger.info("UMAP fitting completed")
        return self

    def transform(self, X: np.ndarray) -> np.ndarray:
        """
        학습된 UMAP으로 데이터 변환

        Args:
            X: 입력 데이터 (n_samples, n_features)

        Returns:
            변환된 데이터 (n_samples, n_components)
        """
        if self.reducer is None:
            raise ValueError("UMAP model not fitted. Call fit() first.")

        return self.reducer.transform(X)

    def fit_transform(self, X: np.ndarray) -> np.ndarray:
        """
        데이터 학습 및 변환

        Args:
            X: 입력 데이터 (n_samples, n_features)

        Returns:
            변환된 데이터 (n_samples, n_components)
        """
        logger.info(f"Fitting and transforming UMAP with {X.shape[0]} samples")

        self.reducer = umap.UMAP(
            n_components=self.n_components,
            n_neighbors=min(self.n_neighbors, X.shape[0] - 1),
            min_dist=self.min_dist,
            metric=self.metric,
            random_state=self.random_state,
            verbose=False
        )

        self.embedding_ = self.reducer.fit_transform(X)
        logger.info(f"UMAP transformation completed: {self.embedding_.shape}")

        return self.embedding_

    def get_embedding_list(self) -> List[List[float]]:
        """
        임베딩을 리스트로 반환 (JSON 직렬화용)

        Returns:
            [[x, y], ...] 형태의 좌표 리스트
        """
        if self.embedding_ is None:
            raise ValueError("No embedding available. Call fit_transform() first.")

        return self.embedding_.tolist()

    def get_params(self) -> Dict[str, Any]:
        """현재 파라미터 반환"""
        return {
            "n_components": self.n_components,
            "n_neighbors": self.n_neighbors,
            "min_dist": self.min_dist,
            "metric": self.metric,
            "random_state": self.random_state
        }


def reduce_dimensions(
    data: np.ndarray,
    n_components: int = 2,
    n_neighbors: int = 15,
    min_dist: float = 0.1
) -> np.ndarray:
    """
    편의 함수: 차원 축소 실행

    Args:
        data: 입력 데이터
        n_components: 축소할 차원 수
        n_neighbors: 이웃 수
        min_dist: 최소 거리

    Returns:
        축소된 데이터
    """
    reducer = UMAPReducer(
        n_components=n_components,
        n_neighbors=n_neighbors,
        min_dist=min_dist
    )
    return reducer.fit_transform(data)
