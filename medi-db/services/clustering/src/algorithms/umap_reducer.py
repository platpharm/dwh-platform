import logging
from typing import Optional, List, Dict, Any
import numpy as np
import umap

logger = logging.getLogger(__name__)

class UMAPReducer:

    def __init__(
        self,
        n_components: int = 2,
        n_neighbors: int = 15,
        min_dist: float = 0.1,
        metric: str = "euclidean",
        random_state: int = 42
    ):
        self.n_components = n_components
        self.n_neighbors = n_neighbors
        self.min_dist = min_dist
        self.metric = metric
        self.random_state = random_state
        self.reducer: Optional[umap.UMAP] = None
        self.embedding_: Optional[np.ndarray] = None

    def fit(self, X: np.ndarray) -> "UMAPReducer":
        if X.shape[0] == 0:
            raise ValueError("Cannot fit UMAP with 0 samples.")
        if X.shape[0] < 2:
            logger.warning("Only 1 sample provided; skipping UMAP fit.")
            self.embedding_ = np.zeros((1, self.n_components))
            return self

        logger.info(f"Fitting UMAP with {X.shape[0]} samples, {X.shape[1]} features")

        actual_n_neighbors = max(2, min(self.n_neighbors, X.shape[0] - 1))
        self.reducer = umap.UMAP(
            n_components=self.n_components,
            n_neighbors=actual_n_neighbors,
            min_dist=self.min_dist,
            metric=self.metric,
            random_state=self.random_state,
            verbose=False
        )
        self.reducer.fit(X)

        logger.info("UMAP fitting completed")
        return self

    def transform(self, X: np.ndarray) -> np.ndarray:
        if self.reducer is None:
            if self.embedding_ is not None:
                logger.warning("UMAP reducer not available (too few samples); returning zeros.")
                return np.zeros((X.shape[0], self.n_components))
            raise ValueError("UMAP model not fitted. Call fit() first.")

        return self.reducer.transform(X)

    def fit_transform(self, X: np.ndarray) -> np.ndarray:
        if X.shape[0] == 0:
            raise ValueError("Cannot fit_transform UMAP with 0 samples.")
        if X.shape[0] < 2:
            logger.warning("Only 1 sample provided; returning zeros for UMAP coordinates.")
            self.embedding_ = np.zeros((1, self.n_components))
            return self.embedding_

        logger.info(f"Fitting and transforming UMAP with {X.shape[0]} samples")

        actual_n_neighbors = max(2, min(self.n_neighbors, X.shape[0] - 1))
        self.reducer = umap.UMAP(
            n_components=self.n_components,
            n_neighbors=actual_n_neighbors,
            min_dist=self.min_dist,
            metric=self.metric,
            random_state=self.random_state,
            verbose=False
        )

        self.embedding_ = self.reducer.fit_transform(X)
        logger.info(f"UMAP transformation completed: {self.embedding_.shape}")

        return self.embedding_

    def get_embedding_list(self) -> List[List[float]]:
        if self.embedding_ is None:
            raise ValueError("No embedding available. Call fit_transform() first.")

        return self.embedding_.tolist()

    def get_params(self) -> Dict[str, Any]:
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
    if data.shape[0] == 0:
        return np.empty((0, n_components))

    reducer = UMAPReducer(
        n_components=n_components,
        n_neighbors=n_neighbors,
        min_dist=min_dist
    )
    return reducer.fit_transform(data)
