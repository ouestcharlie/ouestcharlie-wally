"""Tests for search filter validation — unknown field rejection."""

from __future__ import annotations

import pytest

from wally.agent import _check_filters


def test_none_filters_accepted() -> None:
    _check_filters(None)  # must not raise


def test_empty_filters_accepted() -> None:
    _check_filters({})  # must not raise


def test_known_fields_accepted() -> None:
    _check_filters({"dateTaken": {"min": "2024"}, "rating": {"min": 4}})


def test_single_unknown_field_raises() -> None:
    with pytest.raises(ValueError, match="Unknown filter field"):
        _check_filters({"mood": "happy"})


def test_multiple_unknown_fields_listed_in_error() -> None:
    with pytest.raises(ValueError) as exc_info:
        _check_filters({"mood": "happy", "weather": "sunny", "dateTaken": {"min": "2024"}})
    msg = str(exc_info.value)
    assert "mood" in msg
    assert "weather" in msg
    assert "dateTaken" not in msg  # known field must not appear in the error


def test_error_message_mentions_list_tool() -> None:
    with pytest.raises(ValueError, match="list_search_fields"):
        _check_filters({"nonexistent": "value"})
