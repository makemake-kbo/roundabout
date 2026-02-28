from __future__ import annotations

from datetime import datetime, timedelta, timezone

from roundabout import orchestrator
from roundabout.bgpp import FetchResult
from roundabout.config import CollectorConfig
from roundabout.gtfs import Stop
from roundabout.utils import format_timestamp


def _base_config(tmp_path) -> CollectorConfig:
    return CollectorConfig(
        base_url="http://example.test",
        stops_csv=tmp_path / "stops.csv",
        routes_csv=tmp_path / "routes.csv",
        trips_csv=tmp_path / "trips.csv",
        stop_times_csv=tmp_path / "stop_times.csv",
        output_dir=tmp_path,
        concurrency=2,
        timeout_s=1.0,
        retries=0,
        interval_s=0.0,
        limit=None,
        stop_codes=None,
        route_short_names=None,
        shuffle=False,
        jsonl_enabled=False,
        clickhouse_enabled=True,
        clickhouse_url="http://localhost:8123",
        clickhouse_database="roundabout",
        clickhouse_user=None,
        clickhouse_password=None,
        clickhouse_batch_size=10,
        clickhouse_timeout_s=2.0,
        bbox_min_lat=None,
        bbox_max_lat=None,
        bbox_min_lon=None,
        bbox_max_lon=None,
        rate_limit_rps=50.0,
        rate_limit_enabled=False,
        vehicle_tracking_enabled=False,
        vehicle_tracking_ttl_cycles=5,
        optimistic_enabled=False,
        calendar_csv=tmp_path / "calendar.csv",
        calendar_dates_csv=tmp_path / "calendar_dates.csv",
        checkpoint_stride=5,
        verification_batch_size=80,
        discovery_interval_cycles=10,
        deviation_threshold_s=120,
        stuck_threshold_cycles=3,
        lost_threshold_cycles=5,
    )


def test_collect_once_dedupes_and_writes_clickhouse(monkeypatch, tmp_path):
    registry: dict[str, list[dict[str, object]]] = {}

    class FakeBatchWriter:
        def __init__(self, client, table: str, batch_size: int = 2000) -> None:
            self._table = table
            registry[table] = []

        def write(self, record: dict[str, object]) -> None:
            registry[self._table].append(record)

        def close(self) -> None:
            return None

    observed_at = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    vehicle_payload = {
        "lineNumber": "5",
        "lineName": "Kalemegdan - Ustanicka",
        "secondsLeft": 60,
        "stationsBetween": 3,
        "garageNo": "P80276",
        "coords": ["44.79194500", "20.51088000"],
    }
    responses = {
        "1": FetchResult(
            stop_code="1",
            observed_at=observed_at,
            payload={"uid": 20001, "vehicles": [vehicle_payload]},
            error=None,
            status=200,
            duration_ms=5,
            attempts=1,
        ),
        "2": FetchResult(
            stop_code="2",
            observed_at=observed_at,
            payload={"uid": 20002, "vehicles": [vehicle_payload]},
            error=None,
            status=200,
            duration_ms=5,
            attempts=1,
        ),
    }

    def fake_fetch_stop(stop_code: str, **_kwargs):
        return responses[stop_code]

    monkeypatch.setattr(orchestrator, "ClickHouseBatchWriter", FakeBatchWriter)
    monkeypatch.setattr(orchestrator, "fetch_stop", fake_fetch_stop)

    stops = [
        Stop(stop_id=20001, stop_code="1", stop_name="Stop A", stop_lat=44.0, stop_lon=20.0),
        Stop(stop_id=20002, stop_code="2", stop_name="Stop B", stop_lat=44.1, stop_lon=20.1),
    ]
    config = _base_config(tmp_path)
    summary = orchestrator.collect_once(stops, config)

    assert summary.predictions == 2
    assert summary.unique_vehicles == 1
    assert len(registry["raw_stop_predictions"]) == 2
    assert len(registry["raw_vehicles"]) == 1
    assert len(registry["raw_cycles"]) == 1

    prediction = registry["raw_stop_predictions"][0]
    expected_arrival = format_timestamp(observed_at + timedelta(seconds=60))
    assert prediction["predicted_arrival_at"] == expected_arrival


def test_collect_once_records_errors(monkeypatch, tmp_path):
    registry: dict[str, list[dict[str, object]]] = {}

    class FakeBatchWriter:
        def __init__(self, client, table: str, batch_size: int = 2000) -> None:
            self._table = table
            registry[table] = []

        def write(self, record: dict[str, object]) -> None:
            registry[self._table].append(record)

        def close(self) -> None:
            return None

    def fake_fetch_stop(stop_code: str, **_kwargs):
        return FetchResult(
            stop_code=stop_code,
            observed_at=datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
            payload=None,
            error="timeout",
            status=None,
            duration_ms=100,
            attempts=1,
        )

    monkeypatch.setattr(orchestrator, "ClickHouseBatchWriter", FakeBatchWriter)
    monkeypatch.setattr(orchestrator, "fetch_stop", fake_fetch_stop)

    stops = [
        Stop(stop_id=20001, stop_code="1", stop_name="Stop A", stop_lat=44.0, stop_lon=20.0),
    ]
    config = _base_config(tmp_path)
    summary = orchestrator.collect_once(stops, config)

    assert summary.predictions == 0
    assert summary.unique_vehicles == 0
    assert summary.errors == 1
    assert len(registry["raw_errors"]) == 1
    error_record = registry["raw_errors"][0]
    assert error_record["error"] == "timeout"
