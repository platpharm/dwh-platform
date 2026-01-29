import sys
import math
import types
from unittest.mock import MagicMock, patch

sys.path.insert(0, "/Users/kimhyeonjin/main/dev/dwh-platform/medi-db")

_pydantic_mock = MagicMock()
_pydantic_mock.BaseModel = type("BaseModel", (), {"__init_subclass__": lambda **kw: None, "model_dump": lambda self, **kw: {}})
_pydantic_mock.Field = lambda **kw: None


class _PydanticBaseModel:
    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        for name, annotation in getattr(cls, "__annotations__", {}).items():
            if not hasattr(cls, name):
                setattr(cls, name, None)

    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)

    def model_dump(self, **kwargs):
        return {k: getattr(self, k, None) for k in self.__annotations__}


_pydantic_mod = types.ModuleType("pydantic")
_pydantic_mod.BaseModel = _PydanticBaseModel
_pydantic_mod.Field = lambda **kw: None
sys.modules["pydantic"] = _pydantic_mod

_fastapi_mod = types.ModuleType("fastapi")
_fastapi_mod.APIRouter = MagicMock
_fastapi_mod.HTTPException = Exception
_fastapi_mod.BackgroundTasks = MagicMock
sys.modules["fastapi"] = _fastapi_mod

_es_mock_module = types.ModuleType("elasticsearch")
_es_mock_module.Elasticsearch = MagicMock
_es_mock_module.helpers = MagicMock()
sys.modules["elasticsearch"] = _es_mock_module

_es_exceptions = types.ModuleType("elasticsearch.exceptions")
_es_exceptions.ConnectionError = type("ConnectionError", (Exception,), {})
sys.modules["elasticsearch.exceptions"] = _es_exceptions

_urllib3_mod = types.ModuleType("urllib3")
_urllib3_mod.disable_warnings = lambda *a, **kw: None
_urllib3_exceptions = types.ModuleType("urllib3.exceptions")
_urllib3_exceptions.InsecureRequestWarning = type("InsecureRequestWarning", (Warning,), {})
_urllib3_mod.exceptions = _urllib3_exceptions
sys.modules["urllib3"] = _urllib3_mod
sys.modules["urllib3.exceptions"] = _urllib3_exceptions

_hdbscan = types.ModuleType("hdbscan")
_hdbscan.HDBSCAN = MagicMock
sys.modules["hdbscan"] = _hdbscan

_umap = types.ModuleType("umap")
_umap.UMAP = MagicMock
sys.modules["umap"] = _umap

_kmodes = types.ModuleType("kmodes")
_kmodes_kprototypes = types.ModuleType("kmodes.kprototypes")
_kmodes_kprototypes.KPrototypes = MagicMock
_kmodes.kprototypes = _kmodes_kprototypes
sys.modules["kmodes"] = _kmodes
sys.modules["kmodes.kprototypes"] = _kmodes_kprototypes

_sklearn = types.ModuleType("sklearn")
sys.modules.setdefault("sklearn", _sklearn)
for submod in ["sklearn.mixture", "sklearn.cluster", "sklearn.preprocessing"]:
    m = types.ModuleType(submod)
    m.GaussianMixture = MagicMock
    m.MiniBatchKMeans = MagicMock
    m.StandardScaler = MagicMock
    sys.modules.setdefault(submod, m)

import numpy as np
import pandas as pd
import pytest

from shared.clients.es_client import ESClient
es_client_patcher = patch("shared.clients.es_client.es_client", new_callable=MagicMock)
es_client_patcher.start()

from services.forecasting.src.models.ranking_calculator import RankingCalculator
from services.targeting.src.matcher.vector_combiner import VectorCombiner
from services.targeting.src.matcher.product_pharmacy_matcher import ProductPharmacyMatcher
from services.clustering.src.api.endpoints import _label_encode


class TestRankingCalculatorCalculateScores:

    def _make_calculator(self, product_cache):
        calc = RankingCalculator()
        calc._product_cache = product_cache
        return calc

    def test_popularity_formula_exact(self):
        product_cache = {
            1: {"name": "ProductA", "category1": "C1", "category2": "C2", "category3": "C3"},
        }
        calc = self._make_calculator(product_cache)

        sales_data = [
            {"product_id": 1, "order_qty": 10, "order_price": 100},
        ]
        product_name_trends = {1: 80.0}
        category_trends = {1: 50.0}

        results = calc._calculate_scores(sales_data, product_name_trends, category_trends)

        assert len(results) == 1
        r = results[0]

        assert r["sales_score"] == 1.0
        assert r["product_trend_score"] == 1.0
        assert r["category_trend_score"] == 1.0

        expected = (1.0 * 0.6) + (1.0 * 0.3) + (1.0 * 0.1)
        assert math.isclose(r["popularity_score"], expected, rel_tol=1e-9)

    def test_highest_sales_gets_score_one(self):
        product_cache = {
            1: {"name": "Low", "category1": "", "category2": "", "category3": ""},
            2: {"name": "High", "category1": "", "category2": "", "category3": ""},
        }
        calc = self._make_calculator(product_cache)

        sales_data = [
            {"product_id": 1, "order_qty": 1, "order_price": 100},
            {"product_id": 2, "order_qty": 10, "order_price": 100},
        ]

        results = calc._calculate_scores(sales_data, {}, {})

        by_id = {r["product_id"]: r for r in results}
        assert by_id[2]["sales_score"] == 1.0
        assert by_id[1]["sales_score"] == pytest.approx(100.0 / 1000.0)

    def test_sales_score_normalized_by_max(self):
        product_cache = {
            1: {"name": "A", "category1": "", "category2": "", "category3": ""},
            2: {"name": "B", "category1": "", "category2": "", "category3": ""},
            3: {"name": "C", "category1": "", "category2": "", "category3": ""},
        }
        calc = self._make_calculator(product_cache)

        sales_data = [
            {"product_id": 1, "order_qty": 2, "order_price": 500},
            {"product_id": 2, "order_qty": 4, "order_price": 500},
            {"product_id": 3, "order_qty": 10, "order_price": 500},
        ]

        results = calc._calculate_scores(sales_data, {}, {})
        by_id = {r["product_id"]: r for r in results}

        max_amount = 10 * 500
        assert by_id[1]["sales_score"] == pytest.approx((2 * 500) / max_amount)
        assert by_id[2]["sales_score"] == pytest.approx((4 * 500) / max_amount)
        assert by_id[3]["sales_score"] == pytest.approx(1.0)

    def test_products_ranked_by_popularity_descending(self):
        product_cache = {
            1: {"name": "Low", "category1": "", "category2": "", "category3": ""},
            2: {"name": "Mid", "category1": "", "category2": "", "category3": ""},
            3: {"name": "High", "category1": "", "category2": "", "category3": ""},
        }
        calc = self._make_calculator(product_cache)

        sales_data = [
            {"product_id": 1, "order_qty": 1, "order_price": 100},
            {"product_id": 2, "order_qty": 5, "order_price": 100},
            {"product_id": 3, "order_qty": 10, "order_price": 100},
        ]

        results = calc._calculate_scores(sales_data, {}, {})
        results.sort(key=lambda x: x["popularity_score"], reverse=True)

        scores = [r["popularity_score"] for r in results]
        assert scores == sorted(scores, reverse=True)
        assert results[0]["product_id"] == 3

    def test_products_without_trends_get_sales_only(self):
        product_cache = {
            1: {"name": "NoTrend", "category1": "", "category2": "", "category3": ""},
        }
        calc = self._make_calculator(product_cache)

        sales_data = [
            {"product_id": 1, "order_qty": 5, "order_price": 200},
        ]

        results = calc._calculate_scores(sales_data, {}, {})
        r = results[0]

        assert r["product_trend_score"] == 0.0
        assert r["category_trend_score"] == 0.0
        expected = 1.0 * 0.6
        assert math.isclose(r["popularity_score"], expected, rel_tol=1e-9)

    def test_custom_weights(self):
        product_cache = {
            1: {"name": "A", "category1": "", "category2": "", "category3": ""},
        }
        calc = self._make_calculator(product_cache)

        sales_data = [{"product_id": 1, "order_qty": 10, "order_price": 100}]
        product_name_trends = {1: 50.0}
        category_trends = {1: 50.0}

        results = calc._calculate_scores(
            sales_data, product_name_trends, category_trends,
            sales_weight=0.5, product_trend_weight=0.3, category_trend_weight=0.2
        )

        r = results[0]
        expected = (1.0 * 0.5) + (1.0 * 0.3) + (1.0 * 0.2)
        assert math.isclose(r["popularity_score"], expected, rel_tol=1e-9)

    def test_product_not_in_cache_excluded(self):
        product_cache = {
            1: {"name": "Cached", "category1": "", "category2": "", "category3": ""},
        }
        calc = self._make_calculator(product_cache)

        sales_data = [
            {"product_id": 1, "order_qty": 5, "order_price": 100},
            {"product_id": 999, "order_qty": 10, "order_price": 100},
        ]

        results = calc._calculate_scores(sales_data, {}, {})
        assert len(results) == 1
        assert results[0]["product_id"] == 1

    def test_empty_sales_data(self):
        calc = self._make_calculator({})
        results = calc._calculate_scores([], {}, {})
        assert results == []

    def test_multiple_orders_same_product_aggregated(self):
        product_cache = {
            1: {"name": "Multi", "category1": "", "category2": "", "category3": ""},
        }
        calc = self._make_calculator(product_cache)

        sales_data = [
            {"product_id": 1, "order_qty": 3, "order_price": 100},
            {"product_id": 1, "order_qty": 7, "order_price": 100},
        ]

        results = calc._calculate_scores(sales_data, {}, {})
        assert len(results) == 1
        assert results[0]["total_amount"] == pytest.approx(1000.0)
        assert results[0]["total_qty"] == 10

    def test_trend_normalization(self):
        product_cache = {
            1: {"name": "A", "category1": "", "category2": "", "category3": ""},
            2: {"name": "B", "category1": "", "category2": "", "category3": ""},
        }
        calc = self._make_calculator(product_cache)

        sales_data = [
            {"product_id": 1, "order_qty": 5, "order_price": 100},
            {"product_id": 2, "order_qty": 5, "order_price": 100},
        ]

        product_name_trends = {1: 40.0, 2: 80.0}
        category_trends = {1: 30.0, 2: 60.0}

        results = calc._calculate_scores(sales_data, product_name_trends, category_trends)
        by_id = {r["product_id"]: r for r in results}

        assert by_id[2]["product_trend_score"] == pytest.approx(1.0)
        assert by_id[1]["product_trend_score"] == pytest.approx(0.5)
        assert by_id[2]["category_trend_score"] == pytest.approx(1.0)
        assert by_id[1]["category_trend_score"] == pytest.approx(0.5)


class TestVectorCombiner:

    def _make_combiner(self, cw=0.5, rw=0.3, pw=0.2):
        return VectorCombiner(cluster_weight=cw, ranking_weight=rw, pattern_weight=pw)

    def test_combine_scores_all_provided(self):
        vc = self._make_combiner()
        score = vc.combine_scores(0.8, 0.6, 0.4)
        expected = (0.8 * 0.5) + (0.6 * 0.3) + (0.4 * 0.2)
        assert math.isclose(score, expected, rel_tol=1e-9)

    def test_combine_scores_none_pattern_redistributes(self):
        vc = self._make_combiner()
        score = vc.combine_scores(0.8, 0.6, None)

        adjusted_cw = 0.5 + (0.2 * 0.6)
        adjusted_rw = 0.3 + (0.2 * 0.4)
        expected = (0.8 * adjusted_cw) + (0.6 * adjusted_rw)
        assert math.isclose(score, expected, rel_tol=1e-9)

    def test_combine_scores_clamped_to_one(self):
        vc = self._make_combiner()
        score = vc.combine_scores(5.0, 5.0, 5.0)
        assert score == 1.0

    def test_combine_scores_clamped_to_zero(self):
        vc = self._make_combiner()
        score = vc.combine_scores(-5.0, -5.0, -5.0)
        assert score == 0.0

    def test_combine_scores_zeros(self):
        vc = self._make_combiner()
        score = vc.combine_scores(0.0, 0.0, 0.0)
        assert score == 0.0

    def test_cosine_similarity_identical(self):
        vc = self._make_combiner()
        v = np.array([1.0, 2.0, 3.0])
        assert math.isclose(vc.cosine_similarity(v, v), 1.0, rel_tol=1e-9)

    def test_cosine_similarity_orthogonal(self):
        vc = self._make_combiner()
        v1 = np.array([1.0, 0.0])
        v2 = np.array([0.0, 1.0])
        assert math.isclose(vc.cosine_similarity(v1, v2), 0.0, abs_tol=1e-9)

    def test_cosine_similarity_zero_vector(self):
        vc = self._make_combiner()
        v1 = np.array([0.0, 0.0])
        v2 = np.array([1.0, 2.0])
        assert vc.cosine_similarity(v1, v2) == 0.0
        assert vc.cosine_similarity(v2, v1) == 0.0

    def test_euclidean_distance_same(self):
        vc = self._make_combiner()
        v = np.array([1.0, 2.0, 3.0])
        assert vc.euclidean_distance(v, v) == 0.0

    def test_euclidean_distance_known(self):
        vc = self._make_combiner()
        v1 = np.array([0.0, 0.0])
        v2 = np.array([3.0, 4.0])
        assert math.isclose(vc.euclidean_distance(v1, v2), 5.0, rel_tol=1e-9)

    def test_normalize_vector(self):
        vc = self._make_combiner()
        v = np.array([3.0, 4.0])
        normed = vc.normalize_vector(v)
        assert math.isclose(np.linalg.norm(normed), 1.0, rel_tol=1e-9)
        assert math.isclose(normed[0], 0.6, rel_tol=1e-9)
        assert math.isclose(normed[1], 0.8, rel_tol=1e-9)

    def test_normalize_zero_vector(self):
        vc = self._make_combiner()
        v = np.array([0.0, 0.0, 0.0])
        normed = vc.normalize_vector(v)
        np.testing.assert_array_equal(normed, v)

    def test_combine_feature_vectors_equal_weights(self):
        vc = self._make_combiner()
        v1 = np.array([2.0, 4.0])
        v2 = np.array([6.0, 8.0])
        result = vc.combine_feature_vectors([v1, v2])
        np.testing.assert_allclose(result, np.array([4.0, 6.0]))

    def test_combine_feature_vectors_custom_weights(self):
        vc = self._make_combiner()
        v1 = np.array([10.0, 0.0])
        v2 = np.array([0.0, 10.0])
        result = vc.combine_feature_vectors([v1, v2], weights=[3.0, 1.0])
        np.testing.assert_allclose(result, np.array([7.5, 2.5]))

    def test_combine_feature_vectors_padding(self):
        vc = self._make_combiner()
        v1 = np.array([2.0, 4.0])
        v2 = np.array([6.0, 8.0, 10.0])
        result = vc.combine_feature_vectors([v1, v2])
        assert result.shape[0] == 3
        np.testing.assert_allclose(result, np.array([4.0, 6.0, 5.0]))

    def test_combine_feature_vectors_empty(self):
        vc = self._make_combiner()
        result = vc.combine_feature_vectors([])
        assert result.shape[0] == 0

    def test_batch_combine_scores(self):
        vc = self._make_combiner()
        tuples = [
            (0.8, 0.6, 0.4),
            (0.5, 0.5, None),
            (1.0, 1.0, 1.0),
        ]
        results = vc.batch_combine_scores(tuples)
        assert len(results) == 3
        assert results[0] == vc.combine_scores(0.8, 0.6, 0.4)
        assert results[1] == vc.combine_scores(0.5, 0.5, None)
        assert results[2] == 1.0


class TestProductPharmacyMatcher:

    def _make_matcher(self):
        with patch("services.targeting.src.matcher.product_pharmacy_matcher.es_client"):
            matcher = ProductPharmacyMatcher()
        return matcher

    def test_build_cluster_mapping_separates_types(self):
        matcher = self._make_matcher()

        clustering_results = [
            {"entity_type": "product", "entity_id": 1, "cluster_id": 0},
            {"entity_type": "product", "entity_id": 2, "cluster_id": 0},
            {"entity_type": "product", "entity_id": 3, "cluster_id": 1},
            {"entity_type": "pharmacy", "entity_id": 10, "cluster_id": 0},
            {"entity_type": "pharmacy", "entity_id": 11, "cluster_id": 1},
        ]

        product_clusters, pharmacy_clusters = matcher.build_cluster_mapping(clustering_results)

        assert 0 in product_clusters
        assert 1 in product_clusters
        assert set(product_clusters[0]) == {1, 2}
        assert product_clusters[1] == [3]
        assert pharmacy_clusters[0] == [10]
        assert pharmacy_clusters[1] == [11]

    def test_build_cluster_mapping_skips_missing_fields(self):
        matcher = self._make_matcher()

        clustering_results = [
            {"entity_type": "product", "entity_id": 1, "cluster_id": 0},
            {"entity_type": "product", "entity_id": None, "cluster_id": 0},
            {"entity_type": "product", "entity_id": 2, "cluster_id": None},
        ]

        product_clusters, pharmacy_clusters = matcher.build_cluster_mapping(clustering_results)
        assert 0 in product_clusters
        assert len(product_clusters[0]) == 1

    def test_calculate_cluster_affinity_same_cluster(self):
        matcher = self._make_matcher()

        product_clusters = {0: [1, 2, 3]}
        pharmacy_clusters = {0: [10, 11, 12]}

        affinity = matcher.calculate_cluster_affinity(0, 0, product_clusters, pharmacy_clusters)
        base = 0.8
        avg_size = (3 + 3) / 2
        size_factor = min(1.2, max(0.8, 10 / (avg_size + 10) + 0.8))
        expected = min(1.0, base * size_factor)
        assert math.isclose(affinity, expected, rel_tol=1e-9)
        assert affinity > 0.7

    def test_calculate_cluster_affinity_different_cluster(self):
        matcher = self._make_matcher()

        product_clusters = {0: [1], 5: [2]}
        pharmacy_clusters = {0: [10], 5: [11]}

        affinity_close = matcher.calculate_cluster_affinity(0, 1, product_clusters, pharmacy_clusters)
        affinity_far = matcher.calculate_cluster_affinity(0, 5, product_clusters, pharmacy_clusters)

        assert affinity_close > affinity_far

    def test_calculate_cluster_affinity_distance_decreases(self):
        matcher = self._make_matcher()

        product_clusters = {0: [1]}
        pharmacy_clusters = {1: [10], 3: [11], 7: [12]}

        a1 = matcher.calculate_cluster_affinity(0, 1, product_clusters, pharmacy_clusters)
        a3 = matcher.calculate_cluster_affinity(0, 3, product_clusters, pharmacy_clusters)
        a7 = matcher.calculate_cluster_affinity(0, 7, product_clusters, pharmacy_clusters)

        assert a1 > a3
        assert a3 > a7

    def test_calculate_cluster_affinity_floor_at_point_one(self):
        matcher = self._make_matcher()

        product_clusters = {0: [1]}
        pharmacy_clusters = {100: [10]}

        affinity = matcher.calculate_cluster_affinity(0, 100, product_clusters, pharmacy_clusters)
        assert affinity >= 0.1 * 0.8

    def test_get_ranking_score_map(self):
        matcher = self._make_matcher()

        ranking_results = [
            {"product_id": 1, "product_name": "A", "popularity_score": 0.95, "rank": 1},
            {"product_id": 2, "product_name": "B", "popularity_score": 0.80, "rank": 2},
            {"product_id": 3, "product_name": None, "popularity_score": 0.70, "rank": 3},
        ]

        score_map = matcher.get_ranking_score_map(ranking_results)

        assert 1 in score_map
        assert score_map[1]["product_name"] == "A"
        assert score_map[1]["ranking_score"] == 0.95
        assert score_map[1]["rank"] == 1

        assert 2 in score_map
        assert score_map[2]["ranking_score"] == 0.80

        assert 3 not in score_map

    def test_get_ranking_score_map_empty(self):
        matcher = self._make_matcher()

        score_map = matcher.get_ranking_score_map([])
        assert score_map == {}


class TestLabelEncode:

    def test_basic_encoding(self):
        result = _label_encode(["cat", "dog", "cat", "bird"])
        assert result[0] == result[2]
        assert len(set(result)) == 3

    def test_sorted_mapping(self):
        result = _label_encode(["zebra", "apple", "mango"])
        assert result[1] < result[2] < result[0]

    def test_none_values_treated_as_empty(self):
        result = _label_encode(["a", None, "b", None])
        assert result[1] == result[3]

    def test_all_none(self):
        result = _label_encode([None, None, None])
        assert result == [0, 0, 0]

    def test_empty_list(self):
        result = _label_encode([])
        assert result == []

    def test_single_value(self):
        result = _label_encode(["only"])
        assert result == [0]

    def test_none_and_empty_string_same(self):
        result = _label_encode([None, "", "x"])
        assert result[0] == result[1]
        assert result[2] != result[0]
