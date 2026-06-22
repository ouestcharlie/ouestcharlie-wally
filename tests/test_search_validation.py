"""Tests for search filter validation — unknown field rejection and group parsing."""

from __future__ import annotations

import pytest
from ouestcharlie_toolkit.fields import PHOTO_FIELDS

from wally.agent import _parse_filter_node
from wally.searcher import FilterGroup, FilterLeaf


def _parse(raw):
    return _parse_filter_node(raw, PHOTO_FIELDS)


def test_none_equivalent_accepted() -> None:
    group = _parse({})
    assert isinstance(group, FilterGroup)
    assert group.children == []


def test_known_fields_accepted() -> None:
    group = _parse({"dateTaken": {"min": "2024"}, "rating": {"min": 4}})
    assert isinstance(group, FilterGroup)
    assert len(group.children) == 2


def test_single_unknown_field_raises() -> None:
    with pytest.raises(ValueError, match="Unknown filter field"):
        _parse({"mood": "happy"})


def test_multiple_unknown_fields_raise_on_first() -> None:
    with pytest.raises(ValueError, match="Unknown filter field"):
        _parse({"mood": "happy", "weather": "sunny"})


def test_error_message_mentions_list_tool() -> None:
    with pytest.raises(ValueError, match="list_search_fields"):
        _parse({"nonexistent": "value"})


def test_flat_dict_produces_and_group() -> None:
    group = _parse({"make": "nikon", "rating": {"min": 4}})
    assert group.logic == "AND"
    assert all(isinstance(c, FilterLeaf) for c in group.children)


def test_all_key_produces_and_group() -> None:
    group = _parse({"all": [{"make": "nikon"}, {"rating": {"min": 4}}]})
    assert group.logic == "AND"
    assert len(group.children) == 2


def test_any_key_produces_or_group() -> None:
    group = _parse({"any": [{"make": "nikon"}, {"make": "canon"}]})
    assert group.logic == "OR"
    assert len(group.children) == 2


def test_nested_group_parses_correctly() -> None:
    raw = {
        "all": [
            {"dateTaken": {"min": "2024", "max": "2024"}},
            {"any": [{"make": "nikon"}, {"make": "canon"}]},
        ]
    }
    group = _parse(raw)
    assert group.logic == "AND"
    assert len(group.children) == 2
    date_leaf = group.children[0]
    or_sub = group.children[1]
    assert isinstance(date_leaf, FilterLeaf)
    assert date_leaf.field == "dateTaken"
    assert isinstance(or_sub, FilterGroup)
    assert or_sub.logic == "OR"
    assert len(or_sub.children) == 2


def test_unknown_field_inside_group_raises() -> None:
    with pytest.raises(ValueError, match="Unknown filter field"):
        _parse({"any": [{"mood": "happy"}, {"make": "nikon"}]})
