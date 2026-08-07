"""Tests for _parse_date_min / _parse_date_max (MCP date-range parsing)."""

from __future__ import annotations

from datetime import datetime

import pytest

from wally.agent import _parse_date_max, _parse_date_min

# --- Date-only (existing behaviour) ----------------------------------------


def test_min_none() -> None:
    assert _parse_date_min(None) is None


def test_max_none() -> None:
    assert _parse_date_max(None) is None


def test_min_year() -> None:
    assert _parse_date_min("2024") == datetime(2024, 1, 1, 0, 0, 0)


def test_min_year_month() -> None:
    assert _parse_date_min("2024-07") == datetime(2024, 7, 1, 0, 0, 0)


def test_min_full_date() -> None:
    assert _parse_date_min("2024-07-14") == datetime(2024, 7, 14, 0, 0, 0)


def test_max_year() -> None:
    assert _parse_date_max("2024") == datetime(2024, 12, 31, 23, 59, 59)


def test_max_year_month_end_of_month() -> None:
    assert _parse_date_max("2024-02") == datetime(2024, 2, 29, 23, 59, 59)


def test_max_full_date() -> None:
    assert _parse_date_max("2024-07-14") == datetime(2024, 7, 14, 23, 59, 59)


# --- Time-of-day component (OEC#40) ----------------------------------------


def test_min_full_timestamp() -> None:
    assert _parse_date_min("2024-07-14T18:30:15") == datetime(2024, 7, 14, 18, 30, 15)


def test_min_partial_timestamp_hh_mm() -> None:
    assert _parse_date_min("2024-07-14T18:30") == datetime(2024, 7, 14, 18, 30, 0)


def test_min_partial_timestamp_hh() -> None:
    assert _parse_date_min("2024-07-14T18") == datetime(2024, 7, 14, 18, 0, 0)


def test_max_full_timestamp_no_end_of_day_expansion() -> None:
    assert _parse_date_max("2024-07-14T20:00:00") == datetime(2024, 7, 14, 20, 0, 0)


def test_max_partial_timestamp_hh_mm() -> None:
    assert _parse_date_max("2024-07-14T20:00") == datetime(2024, 7, 14, 20, 0, 0)


def test_precise_window_bounds() -> None:
    lo = _parse_date_min("2024-07-14T18:00:00")
    hi = _parse_date_max("2024-07-14T20:00:00")
    assert lo == datetime(2024, 7, 14, 18, 0, 0)
    assert hi == datetime(2024, 7, 14, 20, 0, 0)
    assert lo < hi


# --- Timezone designators are stripped -------------------------------------


def test_min_timestamp_utc_z() -> None:
    assert _parse_date_min("2024-07-14T18:30:00Z") == datetime(2024, 7, 14, 18, 30, 0)


def test_min_timestamp_positive_offset() -> None:
    assert _parse_date_min("2024-07-14T18:30:00+02:00") == datetime(2024, 7, 14, 18, 30, 0)


def test_min_timestamp_negative_offset() -> None:
    assert _parse_date_min("2024-07-14T18:30:00-05:00") == datetime(2024, 7, 14, 18, 30, 0)


def test_min_timestamp_fractional_seconds() -> None:
    assert _parse_date_min("2024-07-14T18:30:15.123") == datetime(2024, 7, 14, 18, 30, 15)


# --- Errors -----------------------------------------------------------------


def test_min_invalid_raises() -> None:
    with pytest.raises(ValueError, match="Invalid date_min"):
        _parse_date_min("not-a-date")


def test_max_invalid_raises() -> None:
    with pytest.raises(ValueError, match="Invalid date_max"):
        _parse_date_max("2024-13-99")
