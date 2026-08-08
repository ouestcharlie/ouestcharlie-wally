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


def test_single_known_field_accepted() -> None:
    leaf = _parse({"dateTaken": {"min": "2024"}})
    assert isinstance(leaf, FilterLeaf)
    assert leaf.field == "dateTaken"


def test_single_unknown_field_raises() -> None:
    with pytest.raises(ValueError, match="Unknown filter field"):
        _parse({"mood": "happy"})


def test_multi_key_flat_dict_raises() -> None:
    with pytest.raises(ValueError, match="all"):
        _parse({"make": "nikon", "rating": {"min": 4}})


def test_multi_key_flat_dict_error_mentions_all() -> None:
    with pytest.raises(ValueError, match='"all"'):
        _parse({"dateTaken": {"min": "2024"}, "rating": {"min": 4}})


def test_error_message_mentions_list_tool() -> None:
    with pytest.raises(ValueError, match="list_search_fields"):
        _parse({"nonexistent": "value"})


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


# ---------------------------------------------------------------------------
# Video field parsing — hasAudio (BOOL), and non-conformant input detection
# ---------------------------------------------------------------------------


def test_has_audio_bool_true_parsed() -> None:
    from wally.searcher import BoolFilter

    leaf = _parse({"hasAudio": True})
    assert isinstance(leaf, FilterLeaf)
    assert leaf.value == BoolFilter(value=True)


def test_has_audio_bool_false_parsed() -> None:
    from wally.searcher import BoolFilter

    leaf = _parse({"hasAudio": False})
    assert leaf.value == BoolFilter(value=False)


def test_has_audio_non_bool_raises() -> None:
    with pytest.raises(ValueError, match="boolean"):
        _parse({"hasAudio": "yes"})


def test_media_type_string_filter_parsed() -> None:
    from wally.searcher import StringFilter

    leaf = _parse({"mediaType": "video"})
    assert leaf.value == StringFilter(value="video")


def test_filters_as_json_string_raises() -> None:
    """A JSON-encoded string instead of an object is detected, not misparsed."""
    with pytest.raises(ValueError, match="JSON object, not a string"):
        _parse('{"tags": "Alpinism"}')


def test_tags_given_string_instead_of_list_raises() -> None:
    """tags expects a list; a bare string is a schema violation, not silently dropped."""
    with pytest.raises(ValueError, match="list of strings"):
        _parse({"tags": "Alpinism"})


def test_string_match_given_non_string_value_raises() -> None:
    with pytest.raises(ValueError, match="must be a string"):
        _parse({"make": {"value": 123}})
