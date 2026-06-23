"""Tests for FTS filter validation and WHERE clause behaviour for TEXT fields."""

from __future__ import annotations

import pytest
from ouestcharlie_toolkit.fields import PHOTO_FIELDS
from ouestcharlie_toolkit.lance_index import FtsFilter

from wally.agent import _build_fts_filter
from wally.searcher import (
    FilterGroup,
    FilterLeaf,
    SearchPredicate,
    StringFilter,
    _build_where_clause,
)

# ---------------------------------------------------------------------------
# _build_fts_filter — validation
# ---------------------------------------------------------------------------


def test_none_returns_none():
    assert _build_fts_filter(None) is None


def test_valid_single_column_returns_fts_filter():
    result = _build_fts_filter({"query": "Canyon", "columns": ["description"]})
    assert isinstance(result, FtsFilter)
    assert result.query == "Canyon"
    assert result.columns == ["description"]


def test_valid_multiple_columns_accepted():
    result = _build_fts_filter({"query": "sunset", "columns": ["description"]})
    assert result.columns == ["description"]


def test_missing_query_raises():
    with pytest.raises(ValueError, match="query must be a non-empty string"):
        _build_fts_filter({"columns": ["description"]})


def test_empty_query_raises():
    with pytest.raises(ValueError, match="query must be a non-empty string"):
        _build_fts_filter({"query": "", "columns": ["description"]})


def test_non_string_query_raises():
    with pytest.raises(ValueError, match="query must be a non-empty string"):
        _build_fts_filter({"query": 42, "columns": ["description"]})


def test_missing_columns_raises():
    with pytest.raises(ValueError, match="columns must be a non-empty list"):
        _build_fts_filter({"query": "Canyon"})


def test_empty_columns_raises():
    with pytest.raises(ValueError, match="columns must be a non-empty list"):
        _build_fts_filter({"query": "Canyon", "columns": []})


def test_non_list_columns_raises():
    with pytest.raises(ValueError, match="columns must be a non-empty list"):
        _build_fts_filter({"query": "Canyon", "columns": "description"})


def test_non_text_column_raises():
    with pytest.raises(ValueError, match="non-TEXT field"):
        _build_fts_filter({"query": "Canyon", "columns": ["make"]})


def test_unknown_column_raises():
    with pytest.raises(ValueError, match="non-TEXT field"):
        _build_fts_filter({"query": "Canyon", "columns": ["nonexistent"]})


def test_error_mentions_list_search_fields():
    with pytest.raises(ValueError, match="list_search_fields"):
        _build_fts_filter({"query": "Canyon", "columns": ["rating"]})


# ---------------------------------------------------------------------------
# _build_where_clause — TEXT fields produce no SQL
# ---------------------------------------------------------------------------


def test_text_field_in_predicate_produces_no_clause():
    """FieldType.TEXT fields in predicate filters are silently skipped."""
    result = _build_where_clause(
        SearchPredicate(
            root=FilterGroup(children=[FilterLeaf("description", StringFilter(value="Canyon"))])
        ),
        PHOTO_FIELDS,
    )
    assert result is None


def test_text_field_does_not_bleed_into_combined_clause():
    """TEXT filter doesn't pollute a combined predicate that has real SQL filters."""
    result = _build_where_clause(
        SearchPredicate(
            root=FilterGroup(
                children=[
                    FilterLeaf("description", StringFilter(value="Canyon")),
                    FilterLeaf("make", StringFilter(value="nikon")),
                ]
            )
        ),
        PHOTO_FIELDS,
    )
    assert result is not None
    assert "description" not in result
    assert "lower(make) LIKE '%nikon%'" in result
