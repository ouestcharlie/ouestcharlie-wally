"""Tests for search filter validation — unknown field rejection and group parsing."""

from __future__ import annotations

import pytest
from ouestcharlie_toolkit.fields import PHOTO_FIELDS, FieldType

from wally.agent import (
    _FIELD_FORMAT,
    _parse_filter_node,
    _resolve_sort_column,
    _validate_sort_order,
)
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


# ---------------------------------------------------------------------------
# sort_by validation — accepts list_search_fields names, rejects the rest
# ---------------------------------------------------------------------------


def _resolve(name):
    return _resolve_sort_column(name, PHOTO_FIELDS)


def test_sort_by_default_resolves_to_date_column() -> None:
    """The tool default (dateTaken) maps to the date_taken LanceDB column."""
    assert _resolve("dateTaken") == "date_taken"


def test_sort_by_camelcase_name_resolves_to_entry_attr() -> None:
    assert _resolve("rating") == "rating"
    assert _resolve("isoSpeed") == "iso_speed"


def test_sort_by_unknown_field_raises() -> None:
    with pytest.raises(ValueError, match="Unknown or unsortable sort field"):
        _resolve("not_a_real_field_xyz")


def test_sort_by_snake_case_column_rejected() -> None:
    """The old snake_case column name is not a valid sort_by key anymore."""
    with pytest.raises(ValueError, match="Unknown or unsortable sort field"):
        _resolve("date_taken")


@pytest.mark.parametrize("name", ["tags", "gps", "description"])
def test_sort_by_non_sortable_field_raises(name: str) -> None:
    with pytest.raises(ValueError, match="Unknown or unsortable sort field"):
        _resolve(name)


def test_sort_error_message_mentions_list_tool() -> None:
    with pytest.raises(ValueError, match="list_search_fields"):
        _resolve("mood")


# ---------------------------------------------------------------------------
# sort_order validation — accepts asc/desc, rejects the rest
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("order", ["asc", "desc"])
def test_sort_order_valid_passes_through(order: str) -> None:
    assert _validate_sort_order(order) == order


@pytest.mark.parametrize("order", ["descending", "ascending", "DESC", "", "up"])
def test_sort_order_invalid_raises(order: str) -> None:
    with pytest.raises(ValueError, match="Invalid sort_order"):
        _validate_sort_order(order)


# ---------------------------------------------------------------------------
# Filter sub-key validation — misspelled/unknown sub-keys must not be dropped
# ---------------------------------------------------------------------------


def test_date_range_from_to_rejected() -> None:
    """`from`/`to` instead of `min`/`max` must error, not silently match all."""
    with pytest.raises(ValueError, match="unknown key"):
        _parse({"dateTaken": {"from": "2024", "to": "2025"}})


def test_date_range_empty_object_rejected() -> None:
    with pytest.raises(ValueError, match="at least one of 'min'/'max'"):
        _parse({"dateTaken": {}})


def test_date_range_non_dict_rejected() -> None:
    with pytest.raises(ValueError, match="expects an object"):
        _parse({"dateTaken": "2024"})


def test_date_range_valid_min_only_accepted() -> None:
    leaf = _parse({"dateTaken": {"min": "2024"}})
    assert isinstance(leaf, FilterLeaf)
    assert leaf.field == "dateTaken"


def test_int_range_unknown_key_rejected() -> None:
    with pytest.raises(ValueError, match="unknown key"):
        _parse({"rating": {"gte": 4}})


def test_int_range_empty_object_rejected() -> None:
    with pytest.raises(ValueError, match="at least one of 'min'/'max'"):
        _parse({"rating": {}})


def test_gps_unknown_key_rejected() -> None:
    with pytest.raises(ValueError, match="unknown key"):
        _parse({"gps": {"lat": 48.0, "lon": 2.0}})


def test_gps_empty_object_rejected() -> None:
    with pytest.raises(ValueError, match="at least one of"):
        _parse({"gps": {}})


def test_string_match_unknown_key_rejected() -> None:
    with pytest.raises(ValueError, match="unknown key"):
        _parse({"make": {"value": "nikon", "match": "exact"}})


def test_string_match_invalid_mode_rejected() -> None:
    with pytest.raises(ValueError, match="mode must be one of"):
        _parse({"make": {"value": "nikon", "mode": "startsWith"}})


def test_string_match_missing_value_rejected() -> None:
    with pytest.raises(ValueError, match="'value' key"):
        _parse({"make": {"mode": "exact"}})


def test_field_format_covers_every_field_type() -> None:
    """Every FieldType must have a filter-format description.

    list_search_fields looks up _FIELD_FORMAT[fdef.type] for each field, so a
    new FieldType member without an entry makes the tool raise KeyError for the
    entire call. This fails loudly here instead.
    """
    assert set(_FIELD_FORMAT) == set(FieldType)
