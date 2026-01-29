import sys
sys.path.insert(0, "/Users/kimhyeonjin/main/dev/dwh-platform/medi-db")

from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch
from typing import Union

import pytest
import pandas as pd


@pytest.fixture
def google_crawler():
    with patch("shared.clients.es_client.es_client"):
        with patch("pytrends.request.TrendReq"):
            from services.crawler.src.crawlers.google_trends import GoogleTrendsCrawler
            crawler = GoogleTrendsCrawler.__new__(GoogleTrendsCrawler)
            crawler.hl = "ko"
            crawler.tz = 540
            crawler.geo = "KR"
            crawler.pytrends = MagicMock()
            return crawler


@pytest.fixture
def naver_crawler():
    with patch("shared.clients.es_client.es_client"):
        from services.crawler.src.crawlers.naver_trends import NaverTrendsCrawler
        crawler = NaverTrendsCrawler.__new__(NaverTrendsCrawler)
        crawler.client_id = "test_id"
        crawler.client_secret = "test_secret"
        return crawler


class TestGoogleGetTimeframe:

    def test_default_30_days(self, google_crawler):
        result = google_crawler._get_timeframe()
        today = datetime.now().strftime("%Y-%m-%d")
        thirty_days_ago = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
        assert result == f"{thirty_days_ago} {today}"

    def test_custom_date_range(self, google_crawler):
        result = google_crawler._get_timeframe("2024-01-01", "2024-06-30")
        assert result == "2024-01-01 2024-06-30"

    def test_only_start_date(self, google_crawler):
        result = google_crawler._get_timeframe(start_date="2024-03-01")
        today = datetime.now().strftime("%Y-%m-%d")
        assert result == f"2024-03-01 {today}"

    def test_only_end_date(self, google_crawler):
        result = google_crawler._get_timeframe(end_date="2024-12-31")
        thirty_days_ago = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
        assert result == f"{thirty_days_ago} 2024-12-31"


class TestGoogleNormalizeProductName:

    def test_removes_parentheses(self, google_crawler):
        result = google_crawler._normalize_product_name_for_search("타이레놀정500mg(아세트아미노펜)")
        assert "아세트아미노펜" not in result
        assert "(" not in result
        assert ")" not in result

    def test_removes_dosage_mg(self, google_crawler):
        result = google_crawler._normalize_product_name_for_search("타이레놀정500mg")
        assert "500" not in result
        assert "mg" not in result

    def test_removes_dosage_ml(self, google_crawler):
        result = google_crawler._normalize_product_name_for_search("판콜에이내복액30ml")
        assert "30" not in result
        assert "ml" not in result

    def test_removes_dosage_form_정(self, google_crawler):
        result = google_crawler._normalize_product_name_for_search("타이레놀정500mg(아세트아미노펜)")
        assert "타이레놀" in result
        assert result.strip() != ""

    def test_removes_film_coated_tablet(self, google_crawler):
        result = google_crawler._normalize_product_name_for_search("아토르바스타틴필름코팅정")
        assert "필름코팅정" not in result
        assert "아토르바스타틴" in result

    def test_removes_soft_capsule(self, google_crawler):
        result = google_crawler._normalize_product_name_for_search("오메가3연질캡슐")
        assert "연질캡슐" not in result

    def test_removes_special_chars(self, google_crawler):
        result = google_crawler._normalize_product_name_for_search("테스트-약품!@#")
        assert "-" not in result
        assert "!" not in result
        assert "@" not in result
        assert "#" not in result

    def test_pankeol_naebokyak(self, google_crawler):
        result = google_crawler._normalize_product_name_for_search("판콜에이내복액")
        assert "판콜에이내복" in result

    def test_empty_string(self, google_crawler):
        assert google_crawler._normalize_product_name_for_search("") == ""

    def test_none_returns_empty(self, google_crawler):
        assert google_crawler._normalize_product_name_for_search(None) == ""

    def test_whitespace_collapsed(self, google_crawler):
        result = google_crawler._normalize_product_name_for_search("테스트  약품  100mg")
        assert "  " not in result


class TestGoogleGetCategoryName:

    def test_single_digit_padded(self, google_crawler):
        result = google_crawler._get_category_name("1")
        assert result == "해열진통소염제"

    def test_two_digit_code(self, google_crawler):
        result = google_crawler._get_category_name("04")
        assert result == "비타민제"

    def test_none_returns_none(self, google_crawler):
        result = google_crawler._get_category_name(None)
        assert result is None

    def test_empty_string_returns_none(self, google_crawler):
        result = google_crawler._get_category_name("")
        assert result is None

    def test_unknown_code_returns_none(self, google_crawler):
        result = google_crawler._get_category_name("99")
        assert result is None

    def test_all_valid_codes(self, google_crawler):
        for code in range(1, 21):
            result = google_crawler._get_category_name(str(code))
            assert result is not None


class TestGoogleParseTrendData:

    def test_pandas_timestamp_dates(self, google_crawler):
        ts1 = pd.Timestamp("2024-01-01")
        ts2 = pd.Timestamp("2024-01-02")
        interest_data = {
            "타이레놀": {ts1: 75, ts2: 80},
        }
        result = google_crawler._parse_trend_data(interest_data)
        assert len(result) == 2
        assert result[0].keyword == "타이레놀"
        assert result[0].source == "google"
        assert result[0].value == 75.0
        assert result[0].date == ts1.to_pydatetime()

    def test_string_dates(self, google_crawler):
        interest_data = {
            "비타민": {"2024-01-01": 50, "2024-01-02": 60},
        }
        result = google_crawler._parse_trend_data(interest_data)
        assert len(result) == 2
        assert result[0].date == datetime(2024, 1, 1)
        assert result[1].date == datetime(2024, 1, 2)

    def test_with_related_data(self, google_crawler):
        ts = pd.Timestamp("2024-01-01")
        interest_data = {"타이레놀": {ts: 75}}
        related_data = {
            "타이레놀": {
                "top": [
                    {"query": "타이레놀 가격", "value": 100},
                    {"query": "타이레놀 효능", "value": 90},
                ],
                "rising": [],
            }
        }
        result = google_crawler._parse_trend_data(interest_data, related_data)
        assert len(result) == 1
        assert result[0].related_keywords == ["타이레놀 가격", "타이레놀 효능"]
        assert result[0].metadata["related_queries"] == related_data["타이레놀"]

    def test_without_related_data(self, google_crawler):
        ts = pd.Timestamp("2024-01-01")
        interest_data = {"타이레놀": {ts: 75}}
        result = google_crawler._parse_trend_data(interest_data)
        assert result[0].related_keywords is None

    def test_related_data_limits_to_10(self, google_crawler):
        ts = pd.Timestamp("2024-01-01")
        interest_data = {"키워드": {ts: 50}}
        top_queries = [{"query": f"query_{i}", "value": i} for i in range(15)]
        related_data = {"키워드": {"top": top_queries, "rising": []}}
        result = google_crawler._parse_trend_data(interest_data, related_data)
        assert len(result[0].related_keywords) == 10

    def test_empty_interest_data(self, google_crawler):
        result = google_crawler._parse_trend_data({})
        assert result == []

    def test_metadata_contains_score(self, google_crawler):
        interest_data = {"test": {"2024-01-01": 42}}
        result = google_crawler._parse_trend_data(interest_data)
        assert result[0].metadata["score"] == 42


class TestGoogleFetchProductTrends:

    def test_builds_keyword_maps(self, google_crawler):
        products = [
            {"product_id": 1, "product_name": "타이레놀정500mg(아세트아미노펜)", "category2": "01"},
            {"product_id": 2, "product_name": "판콜에이내복액", "category2": "12"},
        ]

        google_crawler.fetch_interest_over_time = MagicMock(return_value={})

        interest_data, mapping_list = google_crawler.fetch_product_trends(products)

        product_name_mappings = [m for m in mapping_list if m["keyword_source"] == "product_name"]
        category_mappings = [m for m in mapping_list if m["keyword_source"] == "category"]

        assert len(product_name_mappings) == 2
        assert len(category_mappings) == 2

        assert product_name_mappings[0]["trend_source"] == "google"
        assert category_mappings[0]["trend_source"] == "google"

    def test_mapping_list_contains_product_name_source(self, google_crawler):
        products = [
            {"product_id": 10, "product_name": "테스트약품", "category2": "04"},
        ]
        google_crawler.fetch_interest_over_time = MagicMock(return_value={})

        _, mapping_list = google_crawler.fetch_product_trends(products)

        sources = [m["keyword_source"] for m in mapping_list]
        assert "product_name" in sources
        assert "category" in sources

    def test_mapping_list_contains_category_source(self, google_crawler):
        products = [
            {"product_id": 5, "product_name": "비타민C", "category2": "04"},
        ]
        google_crawler.fetch_interest_over_time = MagicMock(return_value={})

        _, mapping_list = google_crawler.fetch_product_trends(products)

        category_entries = [m for m in mapping_list if m["keyword_source"] == "category"]
        assert len(category_entries) == 1
        assert category_entries[0]["keyword"] == "비타민제"

    def test_skips_products_without_id(self, google_crawler):
        products = [
            {"product_name": "이름만있음", "category2": "01"},
        ]
        google_crawler.fetch_interest_over_time = MagicMock(return_value={})

        _, mapping_list = google_crawler.fetch_product_trends(products)
        assert len(mapping_list) == 0

    def test_avg_trend_value_calculated(self, google_crawler):
        products = [
            {"product_id": 1, "product_name": "테스트", "category2": "01"},
        ]

        def mock_fetch(keywords, start_date=None, end_date=None):
            if "테스트" in keywords:
                return {"테스트": {"2024-01-01": 40, "2024-01-02": 60}}
            return {}

        google_crawler.fetch_interest_over_time = MagicMock(side_effect=mock_fetch)

        _, mapping_list = google_crawler.fetch_product_trends(products)

        name_mapping = [m for m in mapping_list if m["keyword_source"] == "product_name"][0]
        assert name_mapping["avg_trend_value"] == 50.0


class TestNaverGetDateRange:

    def test_default_30_days(self, naver_crawler):
        start, end = naver_crawler._get_date_range()
        today = datetime.now().strftime("%Y-%m-%d")
        thirty_days_ago = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
        assert start == thirty_days_ago
        assert end == today

    def test_custom_range(self, naver_crawler):
        start, end = naver_crawler._get_date_range("2024-01-01", "2024-06-30")
        assert start == "2024-01-01"
        assert end == "2024-06-30"

    def test_only_start_date(self, naver_crawler):
        start, end = naver_crawler._get_date_range(start_date="2024-03-01")
        today = datetime.now().strftime("%Y-%m-%d")
        assert start == "2024-03-01"
        assert end == today


class TestNaverNormalizeProductName:

    def test_removes_parentheses(self, naver_crawler):
        result = naver_crawler._normalize_product_name_for_search("타이레놀정500mg(아세트아미노펜)")
        assert "아세트아미노펜" not in result
        assert "(" not in result

    def test_removes_dosage(self, naver_crawler):
        result = naver_crawler._normalize_product_name_for_search("타이레놀정500mg")
        assert "500" not in result
        assert "mg" not in result

    def test_removes_dosage_forms(self, naver_crawler):
        result = naver_crawler._normalize_product_name_for_search("아토르바스타틴필름코팅정")
        assert "필름코팅정" not in result

    def test_pankeol_naebokyak(self, naver_crawler):
        result = naver_crawler._normalize_product_name_for_search("판콜에이내복액")
        assert "판콜에이내복" in result

    def test_empty_string(self, naver_crawler):
        assert naver_crawler._normalize_product_name_for_search("") == ""

    def test_none_returns_empty(self, naver_crawler):
        assert naver_crawler._normalize_product_name_for_search(None) == ""


class TestNaverGetCategoryName:

    def test_single_digit_padded(self, naver_crawler):
        assert naver_crawler._get_category_name("1") == "해열진통소염제"

    def test_two_digit_code(self, naver_crawler):
        assert naver_crawler._get_category_name("04") == "비타민제"

    def test_none_returns_none(self, naver_crawler):
        assert naver_crawler._get_category_name(None) is None

    def test_empty_string_returns_none(self, naver_crawler):
        assert naver_crawler._get_category_name("") is None


class TestNaverParseTrendData:

    def test_string_date_format(self, naver_crawler):
        interest_data = {
            "타이레놀": {"2024-01-01": 55.0, "2024-01-02": 60.0},
        }
        result = naver_crawler._parse_trend_data(interest_data)
        assert len(result) == 2
        assert result[0].keyword == "타이레놀"
        assert result[0].source == "naver"
        assert result[0].value == 55.0
        assert result[0].date == datetime(2024, 1, 1)

    def test_no_related_keywords(self, naver_crawler):
        interest_data = {"test": {"2024-01-01": 10.0}}
        result = naver_crawler._parse_trend_data(interest_data)
        assert result[0].related_keywords is None

    def test_metadata_contains_score(self, naver_crawler):
        interest_data = {"test": {"2024-01-01": 42.0}}
        result = naver_crawler._parse_trend_data(interest_data)
        assert result[0].metadata["score"] == 42.0

    def test_empty_data(self, naver_crawler):
        result = naver_crawler._parse_trend_data({})
        assert result == []

    def test_multiple_keywords(self, naver_crawler):
        interest_data = {
            "키워드A": {"2024-01-01": 10.0},
            "키워드B": {"2024-01-01": 20.0},
        }
        result = naver_crawler._parse_trend_data(interest_data)
        assert len(result) == 2
        keywords = {r.keyword for r in result}
        assert keywords == {"키워드A", "키워드B"}


class TestTrendDataSchema:

    def test_valid_trend_data(self):
        from shared.models.schemas import TrendData
        data = TrendData(
            keyword="test",
            source="google",
            date=datetime(2024, 1, 1),
            value=75.0,
        )
        assert data.keyword == "test"
        assert data.source == "google"
        assert data.value == 75.0
        assert data.related_keywords is None
        assert data.metadata is None

    def test_trend_data_with_optional_fields(self):
        from shared.models.schemas import TrendData
        data = TrendData(
            keyword="test",
            source="naver",
            date=datetime(2024, 6, 15),
            value=50.0,
            related_keywords=["related1", "related2"],
            metadata={"score": 50},
        )
        assert data.related_keywords == ["related1", "related2"]
        assert data.metadata == {"score": 50}


class TestTrendProductMappingSchema:

    def test_valid_mapping(self):
        from shared.models.schemas import TrendProductMapping
        mapping = TrendProductMapping(
            product_id=1,
            product_name="테스트",
            keyword="테스트키워드",
            keyword_source="product_name",
        )
        assert mapping.product_id == 1
        assert mapping.trend_score == 0.0
        assert mapping.match_score == 0.0
        assert mapping.timestamp is not None

    def test_default_scores(self):
        from shared.models.schemas import TrendProductMapping
        mapping = TrendProductMapping(
            product_id=2,
            product_name="약품",
            keyword="키워드",
            keyword_source="category",
        )
        assert mapping.trend_score == 0.0
        assert mapping.match_score == 0.0


class TestClusteringResultSchema:

    def test_int_entity_id(self):
        from shared.models.schemas import ClusteringResult
        result = ClusteringResult(
            entity_type="product",
            entity_id=123,
            cluster_id=1,
            algorithm="hdbscan",
        )
        assert result.entity_id == 123
        assert isinstance(result.entity_id, int)

    def test_str_entity_id(self):
        from shared.models.schemas import ClusteringResult
        result = ClusteringResult(
            entity_type="pharmacy",
            entity_id="PH-001",
            cluster_id=2,
            algorithm="kmodes",
        )
        assert result.entity_id == "PH-001"
        assert isinstance(result.entity_id, str)

    def test_default_features(self):
        from shared.models.schemas import ClusteringResult
        result = ClusteringResult(
            entity_type="product",
            entity_id=1,
            cluster_id=0,
            algorithm="hdbscan",
        )
        assert result.features == {}
        assert result.umap_coords is None
