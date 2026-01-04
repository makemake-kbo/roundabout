"""Tests for utility functions."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from roundabout.utils import (
    format_timestamp,
    parse_coords,
    parse_float,
    parse_int,
    round_coordinate,
)


class TestParseInt:
    """Tests for parse_int function."""

    def test_parse_valid_int_string(self):
        assert parse_int("123") == 123

    def test_parse_valid_int(self):
        assert parse_int(456) == 456

    def test_parse_negative_int(self):
        assert parse_int("-789") == -789

    def test_parse_invalid_string(self):
        assert parse_int("invalid") is None

    def test_parse_none(self):
        assert parse_int(None) is None

    def test_parse_float_string(self):
        assert parse_int("123.45") is None

    def test_parse_empty_string(self):
        assert parse_int("") is None


class TestParseFloat:
    """Tests for parse_float function."""

    def test_parse_valid_float_string(self):
        assert parse_float("123.45") == 123.45

    def test_parse_valid_float(self):
        assert parse_float(456.78) == 456.78

    def test_parse_int_string(self):
        assert parse_float("123") == 123.0

    def test_parse_negative_float(self):
        assert parse_float("-12.34") == -12.34

    def test_parse_invalid_string(self):
        assert parse_float("invalid") is None

    def test_parse_none(self):
        assert parse_float(None) is None

    def test_parse_empty_string(self):
        assert parse_float("") is None

    def test_parse_scientific_notation(self):
        assert parse_float("1.23e-4") == 0.000123


class TestFormatTimestamp:
    """Tests for format_timestamp function."""

    def test_format_utc_datetime(self):
        dt = datetime(2024, 1, 1, 12, 30, 45, 123456, tzinfo=timezone.utc)
        result = format_timestamp(dt)
        assert result == "2024-01-01T12:30:45.123Z"

    def test_format_with_custom_timespec(self):
        dt = datetime(2024, 1, 1, 12, 30, 45, 123456, tzinfo=timezone.utc)
        result = format_timestamp(dt, timespec="seconds")
        assert result == "2024-01-01T12:30:45Z"

    def test_format_converts_to_utc(self):
        # Create datetime with +02:00 offset
        from datetime import timedelta

        tz_plus2 = timezone(timedelta(hours=2))
        dt = datetime(2024, 1, 1, 14, 30, 45, 0, tzinfo=tz_plus2)
        result = format_timestamp(dt)
        # Should be converted to UTC (14:30 +02:00 = 12:30 UTC)
        assert result == "2024-01-01T12:30:45.000Z"


class TestParseCoords:
    """Tests for parse_coords function."""

    def test_parse_valid_float_list(self):
        result = parse_coords([44.7921, 20.5108])
        assert result == (44.7921, 20.5108)

    def test_parse_valid_string_list(self):
        result = parse_coords(["44.7921", "20.5108"])
        assert result == (44.7921, 20.5108)

    def test_parse_tuple(self):
        result = parse_coords((44.7921, 20.5108))
        assert result == (44.7921, 20.5108)

    def test_parse_missing_longitude(self):
        result = parse_coords([44.7921])
        assert result == (None, None)

    def test_parse_invalid_string(self):
        result = parse_coords("invalid")
        assert result == (None, None)

    def test_parse_none(self):
        result = parse_coords(None)
        assert result == (None, None)

    def test_parse_invalid_lat(self):
        result = parse_coords(["invalid", "20.5108"])
        assert result == (None, 20.5108)

    def test_parse_invalid_lon(self):
        result = parse_coords(["44.7921", "invalid"])
        assert result == (44.7921, None)

    def test_parse_empty_list(self):
        result = parse_coords([])
        assert result == (None, None)


class TestRoundCoordinate:
    """Tests for round_coordinate function."""

    def test_round_default_precision(self):
        result = round_coordinate(44.792145678)
        assert result == 44.79215

    def test_round_custom_precision(self):
        result = round_coordinate(44.792145678, decimal_places=3)
        assert result == 44.792

    def test_round_none(self):
        result = round_coordinate(None)
        assert result is None

    def test_round_zero(self):
        result = round_coordinate(0.0)
        assert result == 0.0

    def test_round_negative(self):
        result = round_coordinate(-44.792145678)
        assert result == -44.79215

    def test_round_no_change_needed(self):
        result = round_coordinate(44.79215)
        assert result == 44.79215
