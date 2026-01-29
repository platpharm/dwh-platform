import sys

sys.path.insert(0, "/Users/kimhyeonjin/main/dev/dwh-platform/medi-db")

from datetime import datetime
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest


@pytest.fixture
def mock_es_client():
    with patch("shared.clients.es_client.es_client", new_callable=MagicMock) as mock_es:
        yield mock_es


class TestProductProcessor:

    @pytest.fixture(autouse=True)
    def setup(self, mock_es_client):
        with patch(
            "services.preprocessor.src.processors.product_processor.es_client",
            mock_es_client,
        ):
            from services.preprocessor.src.processors.product_processor import (
                CATEGORY2_MAPPING,
                ProductProcessor,
            )

            self.processor = ProductProcessor()
            self.processor.es = mock_es_client
            self.CATEGORY2_MAPPING = CATEGORY2_MAPPING

    def test_normalize_product_name_removes_parentheses(self):
        result = self.processor._normalize_product_name("타이레놀정500mg(아세트아미노펜)")
        assert "아세트아미노펜" not in result
        assert "500mg" not in result
        assert "(" not in result
        assert ")" not in result

    def test_normalize_product_name_removes_dosage_and_form(self):
        result = self.processor._normalize_product_name("타이레놀정500mg(아세트아미노펜)")
        assert "500" not in result
        assert "mg" not in result.lower()

    def test_normalize_product_name_removes_unit_with_quantity(self):
        result = self.processor._normalize_product_name("판콜에이내복액(어린이용)")
        assert "어린이용" not in result
        assert "(" not in result

    def test_normalize_product_name_empty_string(self):
        assert self.processor._normalize_product_name("") == ""

    def test_normalize_product_name_none(self):
        assert self.processor._normalize_product_name(None) == ""

    def test_normalize_product_name_preserves_base_name(self):
        result = self.processor._normalize_product_name("타이레놀정500mg(아세트아미노펜)")
        assert "타이레놀" in result

    def test_normalize_product_name_various_units(self):
        for unit in ["mg", "ml", "g", "mcg", "iu"]:
            result = self.processor._normalize_product_name(f"테스트약100{unit}")
            assert unit not in result.lower()

    def test_normalize_ingredient_removes_parentheses(self):
        result = self.processor._normalize_ingredient("아세트아미노펜(Acetaminophen)")
        assert "Acetaminophen" not in result
        assert "(" not in result

    def test_normalize_ingredient_removes_quantities(self):
        result = self.processor._normalize_ingredient("아세트아미노펜 500mg")
        assert "500" not in result
        assert "mg" not in result.lower()

    def test_normalize_ingredient_empty(self):
        assert self.processor._normalize_ingredient("") == ""

    def test_normalize_ingredient_none(self):
        assert self.processor._normalize_ingredient(None) == ""

    def test_normalize_ingredient_preserves_commas(self):
        result = self.processor._normalize_ingredient("성분A, 성분B")
        assert "," in result

    def test_clean_data_drops_rows_without_id(self):
        df = pd.DataFrame(
            [
                {"id": None, "name": "테스트약"},
                {"id": 1, "name": "정상약"},
            ]
        )
        result = self.processor._clean_data(df)
        assert len(result) == 1
        assert result.iloc[0]["name"] == "정상약"

    def test_clean_data_drops_rows_without_name(self):
        df = pd.DataFrame(
            [
                {"id": 1, "name": None},
                {"id": 2, "name": "정상약"},
            ]
        )
        result = self.processor._clean_data(df)
        assert len(result) == 1
        assert result.iloc[0]["id"] == 2

    def test_clean_data_strips_text_columns(self):
        df = pd.DataFrame(
            [
                {
                    "id": 1,
                    "name": "  테스트약  ",
                    "efficacy": "  효능  ",
                    "ingredient": "  성분  ",
                }
            ]
        )
        result = self.processor._clean_data(df)
        assert result.iloc[0]["name"] == "테스트약"
        assert result.iloc[0]["efficacy"] == "효능"
        assert result.iloc[0]["ingredient"] == "성분"

    def test_clean_data_fills_na_text_columns(self):
        df = pd.DataFrame(
            [{"id": 1, "name": "테스트약", "efficacy": None, "ingredient": None}]
        )
        result = self.processor._clean_data(df)
        assert result.iloc[0]["efficacy"] == ""
        assert result.iloc[0]["ingredient"] == ""

    def test_enrich_data_maps_category2_codes(self):
        self.processor.es.scroll_search.return_value = []
        df = pd.DataFrame(
            [
                {"id": 1, "name": "약1", "category2": "01", "vendor_id": None},
                {"id": 2, "name": "약2", "category2": "04", "vendor_id": None},
            ]
        )
        result = self.processor._enrich_data(df)
        assert result.iloc[0]["category2_name"] == "해열진통소염제"
        assert result.iloc[1]["category2_name"] == "비타민제"

    def test_enrich_data_unknown_category_defaults_to_기타(self):
        self.processor.es.scroll_search.return_value = []
        df = pd.DataFrame(
            [{"id": 1, "name": "약1", "category2": "99", "vendor_id": None}]
        )
        result = self.processor._enrich_data(df)
        assert result.iloc[0]["category2_name"] == "기타"

    def test_enrich_data_empty_category_defaults_to_기타(self):
        self.processor.es.scroll_search.return_value = []
        df = pd.DataFrame(
            [{"id": 1, "name": "약1", "category2": "", "vendor_id": None}]
        )
        result = self.processor._enrich_data(df)
        assert result.iloc[0]["category2_name"] == "기타"

    def test_enrich_data_category2_with_single_digit(self):
        self.processor.es.scroll_search.return_value = []
        df = pd.DataFrame(
            [{"id": 1, "name": "약1", "category2": "1", "vendor_id": None}]
        )
        result = self.processor._enrich_data(df)
        assert result.iloc[0]["category2_name"] == "해열진통소염제"

    def test_category2_mapping_keys(self):
        assert self.CATEGORY2_MAPPING["01"] == "해열진통소염제"
        assert self.CATEGORY2_MAPPING["04"] == "비타민제"
        assert self.CATEGORY2_MAPPING["03"] == "소화기관용제"
        assert self.CATEGORY2_MAPPING["09"] == "항생물질제제"
        assert self.CATEGORY2_MAPPING["12"] == "호흡기관용제"


class TestPharmacyProcessor:

    @pytest.fixture(autouse=True)
    def setup(self, mock_es_client):
        with patch(
            "services.preprocessor.src.processors.pharmacy_processor.es_client",
            mock_es_client,
        ):
            from services.preprocessor.src.processors.pharmacy_processor import (
                PharmacyProcessor,
            )

            self.processor = PharmacyProcessor()
            self.processor.es = mock_es_client

    def test_normalize_phone_11_digits(self):
        assert self.processor._normalize_phone("01012345678") == "010-1234-5678"

    def test_normalize_phone_10_digits_seoul(self):
        assert self.processor._normalize_phone("0212345678") == "02-1234-5678"

    def test_normalize_phone_10_digits_other(self):
        assert self.processor._normalize_phone("0311234567") == "031-123-4567"

    def test_normalize_phone_9_digits(self):
        assert self.processor._normalize_phone("021234567") == "02-123-4567"

    def test_normalize_phone_empty(self):
        assert self.processor._normalize_phone("") == ""

    def test_normalize_phone_already_formatted(self):
        result = self.processor._normalize_phone("010-1234-5678")
        assert result == "010-1234-5678"

    def test_normalize_phone_with_dashes_input(self):
        result = self.processor._normalize_phone("02-123-4567")
        assert result == "02-123-4567"

    def test_normalize_phone_short_number_returned_as_is(self):
        assert self.processor._normalize_phone("1234") == "1234"

    def test_extract_sido_seoul(self):
        assert self.processor._extract_sido("서울특별시 강남구 역삼동") == "서울"

    def test_extract_sido_gyeonggi(self):
        assert self.processor._extract_sido("경기도 성남시 분당구") == "경기"

    def test_extract_sido_short_form(self):
        assert self.processor._extract_sido("서울 강남구 역삼동") == "서울"

    def test_extract_sido_busan(self):
        assert self.processor._extract_sido("부산광역시 해운대구") == "부산"

    def test_extract_sido_empty(self):
        assert self.processor._extract_sido("") == ""

    def test_extract_sido_none(self):
        assert self.processor._extract_sido(None) == ""

    def test_extract_sido_unknown_address(self):
        assert self.processor._extract_sido("알수없는주소") == ""

    def test_extract_sigungu_seoul(self):
        result = self.processor._extract_sigungu("서울특별시 강남구 역삼동")
        assert result == "강남구"

    def test_extract_sigungu_gyeonggi(self):
        result = self.processor._extract_sigungu("경기도 성남시 분당구")
        assert result in ("성남시", "분당구")

    def test_extract_sigungu_empty(self):
        assert self.processor._extract_sigungu("") == ""

    def test_extract_sigungu_none(self):
        assert self.processor._extract_sigungu(None) == ""

    def test_validate_coordinates_valid(self):
        df = pd.DataFrame(
            [{"id": 1, "name": "약국", "lat": 37.5665, "lng": 126.9780}]
        )
        result = self.processor._validate_coordinates(df)
        assert result.iloc[0]["has_valid_coords"] is True

    def test_validate_coordinates_invalid_lat(self):
        df = pd.DataFrame(
            [{"id": 1, "name": "약국", "lat": 10.0, "lng": 126.9780}]
        )
        result = self.processor._validate_coordinates(df)
        assert result.iloc[0]["has_valid_coords"] is False

    def test_validate_coordinates_invalid_lng(self):
        df = pd.DataFrame(
            [{"id": 1, "name": "약국", "lat": 37.5665, "lng": 200.0}]
        )
        result = self.processor._validate_coordinates(df)
        assert result.iloc[0]["has_valid_coords"] is False

    def test_validate_coordinates_null_values(self):
        df = pd.DataFrame(
            [{"id": 1, "name": "약국", "lat": None, "lng": None}]
        )
        result = self.processor._validate_coordinates(df)
        assert result.iloc[0]["has_valid_coords"] is False

    def test_validate_coordinates_boundary_values(self):
        df = pd.DataFrame(
            [
                {"id": 1, "name": "남단", "lat": 33.0, "lng": 124.0},
                {"id": 2, "name": "북단", "lat": 43.0, "lng": 132.0},
            ]
        )
        result = self.processor._validate_coordinates(df)
        assert result.iloc[0]["has_valid_coords"] is True
        assert result.iloc[1]["has_valid_coords"] is True

    def test_merge_order_statistics_with_data(self):
        df = pd.DataFrame(
            [
                {"id": "pharm1", "name": "약국1"},
                {"id": "pharm2", "name": "약국2"},
            ]
        )
        order_stats = {
            "pharm1": {
                "total_orders": 10,
                "total_amount": 500000.0,
                "avg_order_amount": 50000.0,
                "order_frequency": 0.11,
                "unique_products": 5,
            }
        }
        result = self.processor._merge_order_statistics(df, order_stats)
        pharm1 = result[result["id"] == "pharm1"].iloc[0]
        assert pharm1["total_orders"] == 10
        assert pharm1["total_amount"] == 500000.0
        assert pharm1["unique_products"] == 5

        pharm2 = result[result["id"] == "pharm2"].iloc[0]
        assert pharm2["total_orders"] == 0
        assert pharm2["total_amount"] == 0.0

    def test_merge_order_statistics_empty_stats(self):
        df = pd.DataFrame([{"id": "pharm1", "name": "약국1"}])
        result = self.processor._merge_order_statistics(df, {})
        assert result.iloc[0]["total_orders"] == 0
        assert result.iloc[0]["total_amount"] == 0.0
        assert result.iloc[0]["avg_order_amount"] == 0.0
        assert result.iloc[0]["order_frequency"] == 0.0
        assert result.iloc[0]["unique_products"] == 0


class TestKeywordExtractor:

    @pytest.fixture(autouse=True)
    def setup(self, mock_es_client):
        with patch(
            "services.preprocessor.src.processors.keyword_extractor.es_client",
            mock_es_client,
        ):
            from services.preprocessor.src.processors.keyword_extractor import (
                KeywordExtractor,
            )

            self.extractor = KeywordExtractor()
            self.extractor.es = mock_es_client

    def test_extract_keywords_from_all_fields(self):
        product = {
            "name": "타이레놀정500mg",
            "efficacy": "해열, 진통",
            "ingredient": "아세트아미노펜 500mg",
            "category2": "01",
        }
        keywords = self.extractor.extract_keywords(product)
        assert "name" in keywords
        assert "efficacy" in keywords
        assert "ingredient" in keywords
        assert "category" in keywords

    def test_extract_keywords_empty_product(self):
        keywords = self.extractor.extract_keywords({})
        assert keywords["name"] == []
        assert keywords["efficacy"] == []
        assert keywords["ingredient"] == []
        assert keywords["category"] == []

    def test_extract_name_keywords_removes_stopwords(self):
        from services.preprocessor.src.processors.keyword_extractor import STOPWORDS

        result = self.extractor._extract_name_keywords("타이레놀정500mg")
        for kw in result:
            assert kw not in STOPWORDS

    def test_extract_name_keywords_removes_short_tokens(self):
        result = self.extractor._extract_name_keywords("가 나 타이레놀")
        for kw in result:
            assert len(kw) >= 2

    def test_extract_name_keywords_removes_digits(self):
        result = self.extractor._extract_name_keywords("타이레놀 500 정")
        for kw in result:
            assert not kw.isdigit()

    def test_extract_name_keywords_empty(self):
        assert self.extractor._extract_name_keywords("") == []

    def test_extract_name_keywords_none(self):
        assert self.extractor._extract_name_keywords(None) == []

    def test_calculate_match_score_exact_match(self):
        assert self.extractor._calculate_match_score("타이레놀", "타이레놀") == 1.0

    def test_calculate_match_score_substring(self):
        score = self.extractor._calculate_match_score("타이레놀", "타이레놀정")
        assert 0.0 < score < 1.0
        assert score == len("타이레놀") / len("타이레놀정")

    def test_calculate_match_score_reverse_substring(self):
        score = self.extractor._calculate_match_score("타이레놀정", "타이레놀")
        assert 0.0 < score < 1.0
        assert score == len("타이레놀") / len("타이레놀정")

    def test_calculate_match_score_common_prefix(self):
        score = self.extractor._calculate_match_score("타이레놀", "타이레날")
        assert score > 0.0

    def test_calculate_match_score_no_match(self):
        score = self.extractor._calculate_match_score("타이레놀", "감기약")
        assert score == 0.0

    def test_match_trend_with_product_above_threshold(self):
        product = {"id": 1, "name": "타이레놀"}
        product_keywords = {
            "name": ["타이레놀"],
            "efficacy": ["해열"],
            "ingredient": ["아세트아미노펜"],
            "category": ["해열진통소염제"],
        }
        result = self.extractor.match_trend_with_product(
            "타이레놀", 85.0, product, product_keywords
        )
        assert result is not None
        assert result.match_score >= 0.5
        assert result.product_id == 1
        assert result.trend_score == 85.0

    def test_match_trend_with_product_below_threshold(self):
        product = {"id": 1, "name": "타이레놀"}
        product_keywords = {
            "name": ["타이레놀"],
            "efficacy": [],
            "ingredient": [],
            "category": [],
        }
        result = self.extractor.match_trend_with_product(
            "완전다른키워드", 50.0, product, product_keywords
        )
        assert result is None

    def test_match_trend_with_product_partial_match(self):
        product = {"id": 2, "name": "판콜에이"}
        product_keywords = {
            "name": ["판콜에이"],
            "efficacy": [],
            "ingredient": [],
            "category": [],
        }
        result = self.extractor.match_trend_with_product(
            "판콜", 70.0, product, product_keywords
        )
        if result is not None:
            assert result.match_score >= 0.5

    def test_match_trend_returns_trend_product_mapping_type(self):
        from shared.models.schemas import TrendProductMapping

        product = {"id": 1, "name": "타이레놀"}
        product_keywords = {
            "name": ["타이레놀"],
            "efficacy": [],
            "ingredient": [],
            "category": [],
        }
        result = self.extractor.match_trend_with_product(
            "타이레놀", 90.0, product, product_keywords
        )
        assert isinstance(result, TrendProductMapping)

    def test_extract_category_keywords_valid_code(self):
        result = self.extractor._extract_category_keywords("01")
        assert result == ["해열진통소염제"]

    def test_extract_category_keywords_invalid_code(self):
        result = self.extractor._extract_category_keywords("99")
        assert result == []

    def test_extract_category_keywords_empty(self):
        result = self.extractor._extract_category_keywords("")
        assert result == []

    def test_extract_efficacy_keywords(self):
        result = self.extractor._extract_efficacy_keywords("해열, 진통, 소염")
        assert len(result) >= 2
        assert "해열" in result

    def test_extract_ingredient_keywords(self):
        result = self.extractor._extract_ingredient_keywords(
            "아세트아미노펜 500mg, 카페인 50mg"
        )
        assert len(result) >= 1
        for kw in result:
            assert "mg" not in kw.lower() or len(kw) > 5
