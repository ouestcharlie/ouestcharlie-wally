"""Tests for _build_where_clause — SQL WHERE generation from SearchPredicate."""

from __future__ import annotations

from datetime import UTC, datetime

from ouestcharlie_toolkit.fields import PHOTO_FIELDS

from wally.searcher import (
    CollectionFilter,
    GpsBoxFilter,
    RangeFilter,
    SearchPredicate,
    StringFilter,
    _build_where_clause,
)


def _clause(filters: dict) -> str | None:
    return _build_where_clause(SearchPredicate(filters=filters), PHOTO_FIELDS)


# ---------------------------------------------------------------------------
# Empty predicate
# ---------------------------------------------------------------------------


def test_empty_predicate_returns_none():
    assert _build_where_clause(SearchPredicate(), PHOTO_FIELDS) is None


# ---------------------------------------------------------------------------
# DATE_RANGE — "dateTaken" field
# ---------------------------------------------------------------------------


def test_date_lo_produces_timestamp_clause():
    result = _clause({"dateTaken": RangeFilter(lo=datetime(2024, 1, 1))})
    assert "date_taken >= TIMESTAMP '2024-01-01 00:00:00'" in result


def test_date_hi_produces_timestamp_clause():
    result = _clause({"dateTaken": RangeFilter(hi=datetime(2024, 12, 31, 23, 59, 59))})
    assert "date_taken <= TIMESTAMP '2024-12-31 23:59:59'" in result


def test_date_both_bounds_produce_two_clauses():
    result = _clause({"dateTaken": RangeFilter(lo=datetime(2024, 1, 1), hi=datetime(2024, 12, 31))})
    assert "date_taken >=" in result
    assert "date_taken <=" in result


def test_date_none_bounds_produce_no_clause():
    """RangeFilter() with no bounds contributes nothing."""
    result = _clause({"dateTaken": RangeFilter()})
    assert result is None


def test_date_timezone_stripped_before_format():
    """Timezone-aware lo is treated as naive in the SQL literal."""

    dt = datetime(2024, 6, 15, 10, 30, tzinfo=UTC)
    result = _clause({"dateTaken": RangeFilter(lo=dt)})
    assert "TIMESTAMP '2024-06-15 10:30:00'" in result


# ---------------------------------------------------------------------------
# INT_RANGE — rating / width / height / orientation
# ---------------------------------------------------------------------------


def test_rating_lo():
    result = _clause({"rating": RangeFilter(lo=3)})
    assert "rating >= 3" in result


def test_rating_hi():
    result = _clause({"rating": RangeFilter(hi=5)})
    assert "rating <= 5" in result


def test_rating_both_bounds():
    result = _clause({"rating": RangeFilter(lo=3, hi=5)})
    assert "rating >= 3" in result
    assert "rating <= 5" in result


def test_width_lo():
    result = _clause({"width": RangeFilter(lo=1920)})
    assert "width >= 1920" in result


def test_height_hi():
    result = _clause({"height": RangeFilter(hi=1080)})
    assert "height <= 1080" in result


def test_orientation_lo():
    result = _clause({"orientation": RangeFilter(lo=1)})
    assert "orientation >= 1" in result


# ---------------------------------------------------------------------------
# STRING_COLLECTION — tags
# ---------------------------------------------------------------------------


def test_single_tag_produces_array_has_clause():
    result = _clause({"tags": CollectionFilter(values=("travel",))})
    assert "array_has(tags, 'travel')" in result


def test_multiple_tags_each_produce_a_clause():
    result = _clause({"tags": CollectionFilter(values=("travel", "paris"))})
    assert "array_has(tags, 'travel')" in result
    assert "array_has(tags, 'paris')" in result


def test_tag_with_single_quote_is_escaped():
    result = _clause({"tags": CollectionFilter(values=("it's",))})
    assert "array_has(tags, 'it''s')" in result


# ---------------------------------------------------------------------------
# STRING_MATCH — make / model
# ---------------------------------------------------------------------------


def test_make_produces_lower_like_clause():
    result = _clause({"make": StringFilter(value="nikon")})
    assert "lower(make) LIKE '%nikon%'" in result


def test_make_value_is_lowercased():
    result = _clause({"make": StringFilter(value="NIKON")})
    assert "lower(make) LIKE '%nikon%'" in result


def test_model_produces_lower_like_clause():
    result = _clause({"model": StringFilter(value="r5")})
    assert "lower(model) LIKE '%r5%'" in result


def test_string_value_with_quote_is_escaped():
    result = _clause({"make": StringFilter(value="brand's")})
    assert "lower(make) LIKE '%brand''s%'" in result


# ---------------------------------------------------------------------------
# GPS_BOX
# ---------------------------------------------------------------------------


def test_gps_always_adds_null_check_even_with_no_bounds():
    """All-None GpsBoxFilter still rejects photos with no GPS data."""
    result = _clause({"gps": GpsBoxFilter()})
    assert "gps_lat IS NOT NULL" in result
    assert "gps_lon IS NOT NULL" in result


def test_gps_min_lat():
    result = _clause({"gps": GpsBoxFilter(min_lat=48.0)})
    assert "gps_lat >= 48.0" in result


def test_gps_max_lat():
    result = _clause({"gps": GpsBoxFilter(max_lat=49.0)})
    assert "gps_lat <= 49.0" in result


def test_gps_min_lon():
    result = _clause({"gps": GpsBoxFilter(min_lon=2.0)})
    assert "gps_lon >= 2.0" in result


def test_gps_max_lon():
    result = _clause({"gps": GpsBoxFilter(max_lon=3.0)})
    assert "gps_lon <= 3.0" in result


def test_gps_full_bbox_produces_all_clauses():
    result = _clause({"gps": GpsBoxFilter(min_lat=48.0, max_lat=49.0, min_lon=2.0, max_lon=3.0)})
    assert "gps_lat IS NOT NULL" in result
    assert "gps_lat >= 48.0" in result
    assert "gps_lat <= 49.0" in result
    assert "gps_lon >= 2.0" in result
    assert "gps_lon <= 3.0" in result


# ---------------------------------------------------------------------------
# Combined filters
# ---------------------------------------------------------------------------


def test_combined_date_and_rating_joined_with_and():
    result = _clause(
        {"dateTaken": RangeFilter(lo=datetime(2024, 1, 1)), "rating": RangeFilter(lo=4)}
    )
    assert "date_taken >=" in result
    assert "rating >= 4" in result
    assert " AND " in result


def test_combined_tag_and_gps():
    result = _clause(
        {"tags": CollectionFilter(values=("travel",)), "gps": GpsBoxFilter(min_lat=48.0)}
    )
    assert "array_has(tags, 'travel')" in result
    assert "gps_lat IS NOT NULL" in result
    assert "gps_lat >= 48.0" in result


def test_combined_make_and_rating():
    result = _clause({"make": StringFilter(value="canon"), "rating": RangeFilter(lo=3, hi=5)})
    assert "lower(make) LIKE '%canon%'" in result
    assert "rating >= 3" in result
    assert "rating <= 5" in result
