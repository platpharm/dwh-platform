import sys
sys.path.insert(0, "/Users/kimhyeonjin/main/dev/dwh-platform/medi-db")

import pytest
import numpy as np

from services.clustering.src.algorithms.k_prototype import KPrototypeClusterer
from services.clustering.src.algorithms.minibatch_kmeans import MiniBatchKMeansClusterer
from services.clustering.src.algorithms.gmm_cluster import GMMClusterer
from services.clustering.src.algorithms.hdbscan_cluster import HDBSCANClusterer
from services.clustering.src.algorithms.umap_reducer import UMAPReducer


def generate_well_separated_numerical_data(
    n_per_cluster=50, n_features=5, n_clusters=3, seed=42
):
    rng = np.random.RandomState(seed)
    centers = np.array([
        [i * 20] * n_features for i in range(n_clusters)
    ], dtype=float)
    blocks = []
    for center in centers:
        block = rng.randn(n_per_cluster, n_features) * 0.5 + center
        blocks.append(block)
    return np.vstack(blocks)


def generate_mixed_data(n_per_cluster=50, seed=42):
    rng = np.random.RandomState(seed)
    categories = ["A", "B", "C"]
    rows = []
    for i, cat in enumerate(categories):
        for _ in range(n_per_cluster):
            numerical_1 = rng.randn() * 0.5 + i * 20
            numerical_2 = rng.randn() * 0.5 + i * 15
            rows.append([numerical_1, numerical_2, cat])
    return np.array(rows, dtype=object)


class TestKPrototypeClusterer:

    def test_fit_clusters_mixed_data(self):
        X = generate_mixed_data(n_per_cluster=30)
        clusterer = KPrototypeClusterer(n_clusters=3, n_init=3, max_iter=50)
        clusterer.fit(X, categorical_indices=[2])
        assert clusterer.labels_ is not None
        assert len(clusterer.labels_) == 90
        assert set(clusterer.labels_) == {0, 1, 2}

    def test_produces_correct_number_of_clusters(self):
        X = generate_mixed_data(n_per_cluster=30)
        for k in [2, 3]:
            clusterer = KPrototypeClusterer(n_clusters=k, n_init=3, max_iter=50)
            clusterer.fit(X, categorical_indices=[2])
            unique_labels = set(clusterer.labels_)
            assert len(unique_labels) == k

    def test_detect_categorical_columns(self):
        rng = np.random.RandomState(42)
        n_samples = 200
        numerical_col = rng.randn(n_samples)
        categorical_col = rng.choice(["X", "Y", "Z"], size=n_samples)
        low_unique_col = rng.choice([1.0, 2.0, 3.0], size=n_samples)
        X = np.column_stack([numerical_col, categorical_col, low_unique_col])
        clusterer = KPrototypeClusterer()
        detected = clusterer._detect_categorical(X)
        assert 1 in detected
        assert 2 in detected
        assert 0 not in detected

    def test_single_sample_returns_cluster_zero(self):
        X = np.array([[1.0, 2.0, "A"]], dtype=object)
        clusterer = KPrototypeClusterer(n_clusters=3)
        clusterer.fit(X, categorical_indices=[2])
        assert clusterer.labels_[0] == 0
        assert clusterer.cost_ == 0.0
        np.testing.assert_array_equal(clusterer.umap_coords_, np.zeros((1, 2)))

    def test_empty_dataset_raises_value_error(self):
        X = np.empty((0, 3))
        clusterer = KPrototypeClusterer()
        with pytest.raises(ValueError, match="Cannot cluster empty dataset"):
            clusterer.fit(X)

    def test_get_cluster_summary_structure(self):
        X = generate_mixed_data(n_per_cluster=20)
        clusterer = KPrototypeClusterer(n_clusters=3, n_init=3, max_iter=50)
        clusterer.fit(X, categorical_indices=[2])
        summary = clusterer.get_cluster_summary()
        assert "n_clusters" in summary
        assert "n_samples" in summary
        assert "cluster_sizes" in summary
        assert "cost" in summary
        assert "categorical_indices" in summary
        assert summary["n_samples"] == 60
        assert summary["n_clusters"] == 3
        total_in_clusters = sum(summary["cluster_sizes"].values())
        assert total_in_clusters == 60

    def test_get_results_contains_cluster_id_and_umap_coords(self):
        X = generate_mixed_data(n_per_cluster=20)
        clusterer = KPrototypeClusterer(n_clusters=3, n_init=3, max_iter=50)
        clusterer.fit(X, categorical_indices=[2])
        results = clusterer.get_results()
        assert len(results) == 60
        for result in results:
            assert "cluster_id" in result
            assert "umap_coords" in result
            assert isinstance(result["cluster_id"], int)
            assert len(result["umap_coords"]) == 2


class TestMiniBatchKMeansClusterer:

    def test_fit_clusters_numerical_data(self):
        X = generate_well_separated_numerical_data(n_per_cluster=40, n_clusters=3)
        clusterer = MiniBatchKMeansClusterer(n_clusters=3, n_init=3)
        clusterer.fit(X)
        assert clusterer.labels_ is not None
        assert len(set(clusterer.labels_)) == 3
        assert len(clusterer.labels_) == 120

    def test_handles_nan_and_inf_values(self):
        X = generate_well_separated_numerical_data(n_per_cluster=30, n_clusters=3)
        X[0, 0] = np.nan
        X[1, 1] = np.inf
        X[2, 2] = -np.inf
        clusterer = MiniBatchKMeansClusterer(n_clusters=3, n_init=3)
        clusterer.fit(X)
        assert clusterer.labels_ is not None
        assert not np.any(np.isnan(clusterer.labels_))

    def test_find_optimal_clusters_returns_reasonable_value(self):
        X = generate_well_separated_numerical_data(n_per_cluster=40, n_clusters=3)
        clusterer = MiniBatchKMeansClusterer()
        optimal_k = clusterer.find_optimal_clusters(X, min_clusters=2, max_clusters=6)
        assert 2 <= optimal_k <= 6

    def test_single_sample(self):
        X = np.array([[1.0, 2.0, 3.0]])
        clusterer = MiniBatchKMeansClusterer(n_clusters=3)
        clusterer.fit(X)
        assert clusterer.labels_[0] == 0
        assert clusterer.inertia_ == 0.0
        np.testing.assert_array_equal(clusterer.umap_coords_, np.zeros((1, 2)))

    def test_partial_fit_works_incrementally(self):
        X_batch1 = generate_well_separated_numerical_data(n_per_cluster=20, n_clusters=3, seed=1)
        X_batch2 = generate_well_separated_numerical_data(n_per_cluster=20, n_clusters=3, seed=2)
        clusterer = MiniBatchKMeansClusterer(n_clusters=3)
        clusterer.partial_fit(X_batch1)
        assert clusterer.clusterer is not None
        clusterer.partial_fit(X_batch2)
        assert clusterer.clusterer.cluster_centers_ is not None

    def test_get_cluster_centers_returns_inverse_transformed(self):
        X = generate_well_separated_numerical_data(n_per_cluster=40, n_clusters=3)
        clusterer = MiniBatchKMeansClusterer(n_clusters=3, n_init=3)
        clusterer.fit(X)
        centers = clusterer.get_cluster_centers()
        assert len(centers) == 3
        assert len(centers[0]) == 5
        center_means = sorted([np.mean(c) for c in centers])
        assert center_means[0] < center_means[1] < center_means[2]

    def test_empty_dataset_raises_value_error(self):
        X = np.empty((0, 3))
        clusterer = MiniBatchKMeansClusterer()
        with pytest.raises(ValueError, match="Cannot cluster empty dataset"):
            clusterer.fit(X)


class TestGMMClusterer:

    def test_fit_clusters_with_gaussian_mixture(self):
        X = generate_well_separated_numerical_data(n_per_cluster=40, n_clusters=3)
        clusterer = GMMClusterer(n_components=3, n_init=3)
        clusterer.fit(X)
        assert clusterer.labels_ is not None
        assert len(set(clusterer.labels_)) == 3
        assert clusterer.bic_ is not None
        assert clusterer.aic_ is not None

    def test_predict_proba_returns_valid_distributions(self):
        X = generate_well_separated_numerical_data(n_per_cluster=40, n_clusters=3)
        clusterer = GMMClusterer(n_components=3, n_init=3)
        clusterer.fit(X)
        probs = clusterer.predict_proba(X)
        assert probs.shape == (120, 3)
        row_sums = probs.sum(axis=1)
        np.testing.assert_allclose(row_sums, 1.0, atol=1e-6)
        assert np.all(probs >= 0)
        assert np.all(probs <= 1)

    def test_find_optimal_components(self):
        X = generate_well_separated_numerical_data(n_per_cluster=40, n_clusters=3)
        clusterer = GMMClusterer()
        optimal_n = clusterer.find_optimal_components(X, min_components=2, max_components=6)
        assert 2 <= optimal_n <= 6

    def test_find_optimal_components_with_aic(self):
        X = generate_well_separated_numerical_data(n_per_cluster=40, n_clusters=3)
        clusterer = GMMClusterer()
        optimal_n = clusterer.find_optimal_components(
            X, min_components=2, max_components=6, criterion="aic"
        )
        assert 2 <= optimal_n <= 6

    def test_get_results_includes_probabilities(self):
        X = generate_well_separated_numerical_data(n_per_cluster=20, n_clusters=3)
        clusterer = GMMClusterer(n_components=3, n_init=3)
        clusterer.fit(X)
        results = clusterer.get_results()
        assert len(results) == 60
        for result in results:
            assert "cluster_id" in result
            assert "probability" in result
            assert "cluster_probabilities" in result
            assert 0 <= result["probability"] <= 1
            assert len(result["cluster_probabilities"]) == 3


class TestHDBSCANClusterer:

    def test_fit_clusters_with_density(self):
        X = generate_well_separated_numerical_data(n_per_cluster=50, n_clusters=3)
        clusterer = HDBSCANClusterer(min_cluster_size=5, use_umap=False)
        clusterer.fit(X)
        assert clusterer.labels_ is not None
        assert len(clusterer.labels_) == 150
        non_noise_labels = set(l for l in clusterer.labels_ if l != -1)
        assert len(non_noise_labels) >= 1

    def test_handles_noise_points(self):
        rng = np.random.RandomState(42)
        cluster_data = generate_well_separated_numerical_data(
            n_per_cluster=40, n_clusters=2, n_features=3
        )
        noise = rng.uniform(-100, 100, size=(10, 3))
        X = np.vstack([cluster_data, noise])
        clusterer = HDBSCANClusterer(min_cluster_size=5, use_umap=False)
        clusterer.fit(X)
        assert -1 in clusterer.labels_
        summary = clusterer.get_cluster_summary()
        assert summary["n_noise"] > 0
        assert summary["noise_ratio"] > 0

    def test_get_results_structure(self):
        X = generate_well_separated_numerical_data(n_per_cluster=30, n_clusters=3)
        clusterer = HDBSCANClusterer(min_cluster_size=5, use_umap=False)
        clusterer.fit(X)
        results = clusterer.get_results()
        assert len(results) == 90
        for result in results:
            assert "cluster_id" in result
            assert "probability" in result
            assert 0 <= result["probability"] <= 1


class TestUMAPReducer:

    def test_reduces_dimensions_correctly(self):
        rng = np.random.RandomState(42)
        X = rng.randn(50, 10)
        reducer = UMAPReducer(n_components=2)
        embedding = reducer.fit_transform(X)
        assert embedding.shape == (50, 2)

    def test_output_shape_matches_n_components(self):
        rng = np.random.RandomState(42)
        X = rng.randn(50, 10)
        for n_components in [2, 3, 5]:
            reducer = UMAPReducer(n_components=n_components)
            embedding = reducer.fit_transform(X)
            assert embedding.shape == (50, n_components)

    def test_fit_and_transform_separately(self):
        rng = np.random.RandomState(42)
        X_train = rng.randn(50, 10)
        X_test = rng.randn(10, 10)
        reducer = UMAPReducer(n_components=2)
        reducer.fit(X_train)
        transformed = reducer.transform(X_test)
        assert transformed.shape == (10, 2)

    def test_fit_transform_matches_fit_then_transform(self):
        rng = np.random.RandomState(42)
        X = rng.randn(50, 10)
        reducer = UMAPReducer(n_components=2)
        embedding = reducer.fit_transform(X)
        assert reducer.embedding_ is not None
        assert embedding.shape == reducer.embedding_.shape

    def test_single_sample_returns_zeros(self):
        X = np.array([[1.0, 2.0, 3.0]])
        reducer = UMAPReducer(n_components=2)
        embedding = reducer.fit_transform(X)
        np.testing.assert_array_equal(embedding, np.zeros((1, 2)))

    def test_empty_dataset_raises_value_error(self):
        X = np.empty((0, 5))
        reducer = UMAPReducer()
        with pytest.raises(ValueError, match="Cannot fit_transform UMAP with 0 samples"):
            reducer.fit_transform(X)

    def test_get_embedding_list(self):
        rng = np.random.RandomState(42)
        X = rng.randn(20, 5)
        reducer = UMAPReducer(n_components=2)
        reducer.fit_transform(X)
        embedding_list = reducer.get_embedding_list()
        assert isinstance(embedding_list, list)
        assert len(embedding_list) == 20
        assert len(embedding_list[0]) == 2
