"""Tests for data transformation functions."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from roundabout.bgpp import FetchResult
from roundabout.gtfs import Stop
from roundabout.transformers import (
    build_error_record,
    build_output_paths,
    build_prediction_record,
    build_vehicle_key,
    build_vehicle_record,
    normalize_vehicle,
)


class TestNormalizeVehicle:
    """Tests for normalize_vehicle function."""

    def test_normalize_full_vehicle(self):
        vehicle = {
            "lineNumber": "5",
            "lineName": "Kalemegdan - Ustanicka",
            "direction": "A",
            "secondsLeft": 120,
            "stationsBetween": 3,
            "garageNo": "P80276",
            "coords": [44.7921, 20.5108],
        }
        result = normalize_vehicle(vehicle)
        assert result == {
            "line_number": "5",
            "line_name": "Kalemegdan - Ustanicka",
            "direction": "A",
            "seconds_left": 120,
            "stations_between": 3,
            "vehicle_id": "P80276",
            "vehicle_lat": 44.7921,
            "vehicle_lon": 20.5108,
        }

    def test_normalize_missing_fields(self):
        vehicle = {}
        result = normalize_vehicle(vehicle)
        assert result == {
            "line_number": None,
            "line_name": None,
            "direction": None,
            "seconds_left": None,
            "stations_between": None,
            "vehicle_id": None,
            "vehicle_lat": None,
            "vehicle_lon": None,
        }

    def test_normalize_invalid_coords(self):
        vehicle = {"coords": "invalid"}
        result = normalize_vehicle(vehicle)
        assert result["vehicle_lat"] is None
        assert result["vehicle_lon"] is None

    def test_normalize_numeric_line_number(self):
        vehicle = {"lineNumber": 5}
        result = normalize_vehicle(vehicle)
        assert result["line_number"] == "5"

    def test_normalize_invalid_seconds_left(self):
        vehicle = {"secondsLeft": "invalid"}
        result = normalize_vehicle(vehicle)
        assert result["seconds_left"] is None


class TestBuildVehicleKey:
    """Tests for build_vehicle_key function."""

    def test_key_with_vehicle_id(self):
        key = build_vehicle_key("P80276", "5", "A", 44.7921, 20.5108, "1001")
        assert key == "garage:P80276"

    def test_key_without_vehicle_id_with_coords(self):
        key = build_vehicle_key(None, "5", "A", 44.792145678, 20.510876543, "1001")
        # Should use hash with rounded coordinates
        assert key.startswith("hash:")
        assert len(key) == 5 + 16  # "hash:" + 16 char hash

    def test_key_without_coords(self):
        key = build_vehicle_key(None, "5", "A", None, None, "1001")
        # Should include stop_code in hash when coords missing
        assert key.startswith("hash:")

    def test_key_same_vehicle_same_key(self):
        key1 = build_vehicle_key(None, "5", "A", 44.79215, 20.51088, "1001")
        key2 = build_vehicle_key(None, "5", "A", 44.79215, 20.51088, "1001")
        assert key1 == key2

    def test_key_different_coords_different_key(self):
        key1 = build_vehicle_key(None, "5", "A", 44.79215, 20.51088, "1001")
        key2 = build_vehicle_key(None, "5", "A", 44.79216, 20.51088, "1001")
        assert key1 != key2


class TestBuildPredictionRecord:
    """Tests for build_prediction_record function."""

    def test_build_complete_prediction(self):
        stop = Stop(
            stop_id=20001,
            stop_code="1001",
            stop_name="Test Stop",
            stop_lat=44.0,
            stop_lon=20.0,
        )
        observed_at = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        result = FetchResult(
            stop_code="1001",
            observed_at=observed_at,
            payload={"uid": 123},
            error=None,
            status=200,
            duration_ms=50,
            attempts=1,
        )
        vehicle = {
            "lineNumber": "5",
            "lineName": "Test Line",
            "direction": "A",
            "secondsLeft": 60,
            "stationsBetween": 2,
            "garageNo": "P80276",
            "coords": [44.7921, 20.5108],
        }

        record = build_prediction_record(
            stop=stop, result=result, vehicle=vehicle, cycle_id="20240101T120000Z"
        )

        assert record["cycle_id"] == "20240101T120000Z"
        assert record["stop_id"] == 20001
        assert record["stop_code"] == "1001"
        assert record["api_stop_uid"] == 123
        assert record["line_number"] == "5"
        assert record["seconds_left"] == 60
        assert record["predicted_arrival_at"] == "2024-01-01T12:01:00.000Z"
        assert record["vehicle_key"] == "garage:P80276"

    def test_build_prediction_no_seconds_left(self):
        stop = Stop(20001, "1001", "Test", 44.0, 20.0)
        observed_at = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        result = FetchResult("1001", observed_at, {}, None, 200, 50, 1)
        vehicle = {"lineNumber": "5"}

        record = build_prediction_record(
            stop=stop, result=result, vehicle=vehicle, cycle_id="test"
        )

        assert record["seconds_left"] is None
        assert record["predicted_arrival_at"] is None


class TestBuildVehicleRecord:
    """Tests for build_vehicle_record function."""

    def test_build_vehicle_record(self):
        stop = Stop(20001, "1001", "Test Stop", 44.0, 20.0)
        observed_at = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        result = FetchResult("1001", observed_at, {}, None, 200, 50, 1)

        prediction = {
            "observed_at": "2024-01-01T12:00:00.000Z",
            "cycle_id": "test",
            "vehicle_id": "P80276",
            "vehicle_key": "garage:P80276",
            "line_number": "5",
            "line_name": "Test Line",
            "direction": "A",
            "vehicle_lat": 44.7921,
            "vehicle_lon": 20.5108,
            "seconds_left": 60,
            "stations_between": 2,
        }

        record = build_vehicle_record(stop=stop, result=result, prediction=prediction)

        assert record["vehicle_key"] == "garage:P80276"
        assert record["source_stop_id"] == 20001
        assert record["source_stop_code"] == "1001"
        assert record["line_number"] == "5"


class TestBuildErrorRecord:
    """Tests for build_error_record function."""

    def test_build_error_record(self):
        stop = Stop(20001, "1001", "Test Stop", 44.0, 20.0)
        observed_at = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        result = FetchResult(
            stop_code="1001",
            observed_at=observed_at,
            payload=None,
            error="timeout",
            status=None,
            duration_ms=5000,
            attempts=3,
        )

        record = build_error_record(stop=stop, result=result, cycle_id="test")

        assert record["cycle_id"] == "test"
        assert record["stop_id"] == 20001
        assert record["stop_code"] == "1001"
        assert record["error"] == "timeout"
        assert record["http_status"] is None
        assert record["attempts"] == 3
        assert record["duration_ms"] == 5000


class TestBuildOutputPaths:
    """Tests for build_output_paths function."""

    def test_build_output_paths(self):
        output_dir = Path("/data/raw")
        cycle_id = "20240101T120000Z"
        started_at = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)

        paths = build_output_paths(output_dir, cycle_id, started_at)

        assert paths["predictions"] == Path("/data/raw/2024/01/15/stop_predictions_20240101T120000Z.jsonl")
        assert paths["vehicles"] == Path("/data/raw/2024/01/15/vehicles_20240101T120000Z.jsonl")
        assert paths["errors"] == Path("/data/raw/2024/01/15/errors_20240101T120000Z.jsonl")
        assert paths["cycles"] == Path("/data/raw/2024/01/15/cycles.jsonl")

    def test_build_output_paths_different_month(self):
        output_dir = Path("/data/raw")
        cycle_id = "test"
        started_at = datetime(2024, 12, 31, 23, 59, 59, tzinfo=timezone.utc)

        paths = build_output_paths(output_dir, cycle_id, started_at)

        assert str(paths["predictions"]).startswith("/data/raw/2024/12/31/")
