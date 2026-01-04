from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import random
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable

from roundabout.bgpp import DEFAULT_BASE_URL, FetchResult, fetch_stop
from roundabout.clickhouse import ClickHouseBatchWriter, ClickHouseClient, ClickHouseConfig
from roundabout.gtfs import Stop, load_stops
from roundabout.storage import JsonlWriter

LOG = logging.getLogger(__name__)


@dataclass(frozen=True)
class CollectorConfig:
    base_url: str
    stops_csv: Path
    output_dir: Path
    concurrency: int
    timeout_s: float
    retries: int
    interval_s: float
    limit: int | None
    stop_codes: set[str] | None
    shuffle: bool
    clickhouse_enabled: bool
    clickhouse_url: str
    clickhouse_database: str
    clickhouse_user: str | None
    clickhouse_password: str | None
    clickhouse_batch_size: int
    clickhouse_timeout_s: float


@dataclass(frozen=True)
class CycleSummary:
    cycle_id: str
    started_at: datetime
    finished_at: datetime
    stops_total: int
    responses: int
    errors: int
    predictions: int
    unique_vehicles: int

    def as_record(self) -> dict[str, Any]:
        return {
            "cycle_id": self.cycle_id,
            "started_at": _format_ts(self.started_at),
            "finished_at": _format_ts(self.finished_at),
            "stops_total": self.stops_total,
            "responses": self.responses,
            "errors": self.errors,
            "predictions": self.predictions,
            "unique_vehicles": self.unique_vehicles,
        }


def _format_ts(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _parse_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _parse_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _parse_coords(coords: Any) -> tuple[float | None, float | None]:
    if not isinstance(coords, (list, tuple)) or len(coords) < 2:
        return None, None
    lat = _parse_float(coords[0])
    lon = _parse_float(coords[1])
    return lat, lon


def _round_coord(value: float | None) -> float | None:
    if value is None:
        return None
    return round(value, 5)


def _build_vehicle_key(
    vehicle_id: str | None,
    line_number: str | None,
    direction: str | None,
    lat: float | None,
    lon: float | None,
    stop_code: str,
) -> str:
    if vehicle_id:
        return f"garage:{vehicle_id}"
    key_payload: dict[str, Any] = {
        "line_number": line_number,
        "direction": direction,
        "lat": _round_coord(lat),
        "lon": _round_coord(lon),
    }
    if key_payload["lat"] is None or key_payload["lon"] is None:
        key_payload["stop_code"] = stop_code
    raw = json.dumps(key_payload, sort_keys=True)
    digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]
    return f"hash:{digest}"


def _normalize_vehicle(vehicle: dict[str, Any]) -> dict[str, Any]:
    line_number = vehicle.get("lineNumber")
    line_name = vehicle.get("lineName")
    direction = vehicle.get("direction")
    seconds_left = _parse_int(vehicle.get("secondsLeft"))
    stations_between = _parse_int(vehicle.get("stationsBetween"))
    vehicle_id = vehicle.get("garageNo")
    lat, lon = _parse_coords(vehicle.get("coords"))
    return {
        "line_number": str(line_number) if line_number is not None else None,
        "line_name": line_name,
        "direction": str(direction) if direction is not None else None,
        "seconds_left": seconds_left,
        "stations_between": stations_between,
        "vehicle_id": str(vehicle_id) if vehicle_id is not None else None,
        "vehicle_lat": lat,
        "vehicle_lon": lon,
    }


def _prediction_record(
    *,
    stop: Stop,
    result: FetchResult,
    vehicle: dict[str, Any],
    cycle_id: str,
) -> dict[str, Any]:
    normalized = _normalize_vehicle(vehicle)
    seconds_left = normalized["seconds_left"]
    observed_at = result.observed_at
    predicted_arrival_at = None
    if seconds_left is not None:
        predicted_arrival_at = _format_ts(observed_at + timedelta(seconds=seconds_left))
    vehicle_key = _build_vehicle_key(
        normalized["vehicle_id"],
        normalized["line_number"],
        normalized["direction"],
        normalized["vehicle_lat"],
        normalized["vehicle_lon"],
        stop.stop_code,
    )
    return {
        "observed_at": _format_ts(observed_at),
        "cycle_id": cycle_id,
        "stop_id": stop.stop_id,
        "stop_code": stop.stop_code,
        "api_stop_uid": result.payload.get("uid") if isinstance(result.payload, dict) else None,
        "line_number": normalized["line_number"],
        "line_name": normalized["line_name"],
        "direction": normalized["direction"],
        "seconds_left": seconds_left,
        "predicted_arrival_at": predicted_arrival_at,
        "stations_between": normalized["stations_between"],
        "vehicle_id": normalized["vehicle_id"],
        "vehicle_key": vehicle_key,
        "vehicle_lat": normalized["vehicle_lat"],
        "vehicle_lon": normalized["vehicle_lon"],
    }


def _vehicle_record(
    *,
    stop: Stop,
    result: FetchResult,
    prediction: dict[str, Any],
) -> dict[str, Any]:
    return {
        "observed_at": prediction["observed_at"],
        "cycle_id": prediction["cycle_id"],
        "vehicle_id": prediction["vehicle_id"],
        "vehicle_key": prediction["vehicle_key"],
        "line_number": prediction["line_number"],
        "line_name": prediction["line_name"],
        "direction": prediction["direction"],
        "vehicle_lat": prediction["vehicle_lat"],
        "vehicle_lon": prediction["vehicle_lon"],
        "source_stop_id": stop.stop_id,
        "source_stop_code": stop.stop_code,
        "seconds_left": prediction["seconds_left"],
        "stations_between": prediction["stations_between"],
    }


def _error_record(*, stop: Stop, result: FetchResult, cycle_id: str) -> dict[str, Any]:
    return {
        "observed_at": _format_ts(result.observed_at),
        "cycle_id": cycle_id,
        "stop_id": stop.stop_id,
        "stop_code": stop.stop_code,
        "error": result.error,
        "http_status": result.status,
        "attempts": result.attempts,
        "duration_ms": result.duration_ms,
    }


def _build_output_paths(output_dir: Path, cycle_id: str, started_at: datetime) -> dict[str, Path]:
    date_prefix = started_at.strftime("%Y/%m/%d")
    base_dir = output_dir / date_prefix
    return {
        "predictions": base_dir / f"stop_predictions_{cycle_id}.jsonl",
        "vehicles": base_dir / f"vehicles_{cycle_id}.jsonl",
        "errors": base_dir / f"errors_{cycle_id}.jsonl",
        "cycles": base_dir / "cycles.jsonl",
    }


def collect_once(stops: list[Stop], config: CollectorConfig) -> CycleSummary:
    started_at = datetime.now(timezone.utc)
    cycle_id = started_at.strftime("%Y%m%dT%H%M%SZ")
    output_paths = _build_output_paths(config.output_dir, cycle_id, started_at)
    predictions_writer = JsonlWriter(output_paths["predictions"])
    vehicles_writer = JsonlWriter(output_paths["vehicles"])
    errors_writer = JsonlWriter(output_paths["errors"])
    clickhouse_client = None
    clickhouse_predictions = None
    clickhouse_vehicles = None
    clickhouse_errors = None
    clickhouse_cycles = None
    if config.clickhouse_enabled:
        clickhouse_client = ClickHouseClient(
            ClickHouseConfig(
                url=config.clickhouse_url,
                database=config.clickhouse_database,
                user=config.clickhouse_user,
                password=config.clickhouse_password,
                timeout_s=config.clickhouse_timeout_s,
            )
        )
        clickhouse_predictions = ClickHouseBatchWriter(
            clickhouse_client,
            "raw_stop_predictions",
            batch_size=config.clickhouse_batch_size,
        )
        clickhouse_vehicles = ClickHouseBatchWriter(
            clickhouse_client,
            "raw_vehicles",
            batch_size=config.clickhouse_batch_size,
        )
        clickhouse_errors = ClickHouseBatchWriter(
            clickhouse_client,
            "raw_errors",
            batch_size=config.clickhouse_batch_size,
        )
        clickhouse_cycles = ClickHouseBatchWriter(
            clickhouse_client,
            "raw_cycles",
            batch_size=config.clickhouse_batch_size,
        )
    seen_vehicle_keys: set[str] = set()
    predictions_count = 0
    unique_vehicles = 0
    errors = 0
    responses = 0
    try:
        with ThreadPoolExecutor(max_workers=config.concurrency) as executor:
            future_to_stop = {
                executor.submit(
                    fetch_stop,
                    stop.stop_code,
                    base_url=config.base_url,
                    timeout_s=config.timeout_s,
                    retries=config.retries,
                ): stop
                for stop in stops
            }
            for future in as_completed(future_to_stop):
                stop = future_to_stop[future]
                try:
                    result = future.result()
                except Exception as exc:  # pragma: no cover - unexpected
                    errors += 1
                    unexpected_record = {
                        "observed_at": _format_ts(datetime.now(timezone.utc)),
                        "cycle_id": cycle_id,
                        "stop_id": stop.stop_id,
                        "stop_code": stop.stop_code,
                        "error": f"unexpected:{exc}",
                        "http_status": None,
                        "attempts": 0,
                        "duration_ms": 0,
                    }
                    errors_writer.write(unexpected_record)
                    if clickhouse_errors:
                        clickhouse_errors.write(unexpected_record)
                    continue
                responses += 1
                if result.error:
                    errors += 1
                    error_record = _error_record(stop=stop, result=result, cycle_id=cycle_id)
                    errors_writer.write(error_record)
                    if clickhouse_errors:
                        clickhouse_errors.write(error_record)
                    continue
                vehicles = result.payload.get("vehicles") if isinstance(result.payload, dict) else None
                if not isinstance(vehicles, list):
                    vehicles = []
                for vehicle in vehicles:
                    prediction = _prediction_record(
                        stop=stop,
                        result=result,
                        vehicle=vehicle,
                        cycle_id=cycle_id,
                    )
                    predictions_writer.write(prediction)
                    if clickhouse_predictions:
                        clickhouse_predictions.write(prediction)
                    predictions_count += 1
                    if prediction["vehicle_key"] in seen_vehicle_keys:
                        continue
                    seen_vehicle_keys.add(prediction["vehicle_key"])
                    vehicle_record = _vehicle_record(
                        stop=stop,
                        result=result,
                        prediction=prediction,
                    )
                    vehicles_writer.write(vehicle_record)
                    if clickhouse_vehicles:
                        clickhouse_vehicles.write(vehicle_record)
                    unique_vehicles += 1
    finally:
        predictions_writer.close()
        vehicles_writer.close()
        errors_writer.close()
        if clickhouse_predictions:
            clickhouse_predictions.close()
        if clickhouse_vehicles:
            clickhouse_vehicles.close()
        if clickhouse_errors:
            clickhouse_errors.close()
    finished_at = datetime.now(timezone.utc)
    summary = CycleSummary(
        cycle_id=cycle_id,
        started_at=started_at,
        finished_at=finished_at,
        stops_total=len(stops),
        responses=responses,
        errors=errors,
        predictions=predictions_count,
        unique_vehicles=unique_vehicles,
    )
    cycles_writer = JsonlWriter(output_paths["cycles"])
    cycles_writer.write(summary.as_record())
    cycles_writer.close()
    if clickhouse_cycles:
        clickhouse_cycles.write(summary.as_record())
        clickhouse_cycles.close()
    return summary


def collect_forever(stops: list[Stop], config: CollectorConfig) -> None:
    while True:
        started = time.monotonic()
        summary = collect_once(stops, config)
        LOG.info(
            "cycle=%s stops=%s predictions=%s unique_vehicles=%s errors=%s duration_s=%.2f",
            summary.cycle_id,
            summary.stops_total,
            summary.predictions,
            summary.unique_vehicles,
            summary.errors,
            (summary.finished_at - summary.started_at).total_seconds(),
        )
        if config.interval_s <= 0:
            break
        elapsed = time.monotonic() - started
        sleep_for = max(0.0, config.interval_s - elapsed)
        if sleep_for:
            time.sleep(sleep_for)


def _parse_stop_codes(values: Iterable[str]) -> set[str]:
    stop_codes: set[str] = set()
    for value in values:
        for part in value.split(","):
            trimmed = part.strip()
            if trimmed:
                stop_codes.add(trimmed)
    return stop_codes


def parse_args(argv: list[str] | None = None) -> CollectorConfig:
    parser = argparse.ArgumentParser(description="Collect BG++ stop predictions.")
    parser.add_argument("--stops-csv", default="stops-data/stops.csv")
    parser.add_argument("--output-dir", default="data/raw")
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL)
    parser.add_argument("--concurrency", type=int, default=10)
    parser.add_argument("--timeout", type=float, default=10.0)
    parser.add_argument("--retries", type=int, default=2)
    parser.add_argument("--interval", type=float, default=0.0)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--stop-code", action="append", default=[])
    parser.add_argument("--shuffle", action="store_true")
    parser.add_argument("--log-level", default="INFO")
    parser.add_argument(
        "--clickhouse-url",
        default=os.getenv("CLICKHOUSE_URL", "http://localhost:8123"),
    )
    parser.add_argument(
        "--clickhouse-database",
        default=os.getenv("CLICKHOUSE_DB", "roundabout"),
    )
    parser.add_argument(
        "--clickhouse-user",
        default=os.getenv("CLICKHOUSE_USER"),
    )
    parser.add_argument(
        "--clickhouse-password",
        default=os.getenv("CLICKHOUSE_PASSWORD"),
    )
    parser.add_argument(
        "--clickhouse-batch-size",
        type=int,
        default=int(os.getenv("CLICKHOUSE_BATCH_SIZE", "2000")),
    )
    parser.add_argument(
        "--clickhouse-timeout",
        type=float,
        default=float(os.getenv("CLICKHOUSE_TIMEOUT", "10")),
    )
    parser.add_argument("--no-clickhouse", action="store_true")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(message)s",
    )

    stop_codes = _parse_stop_codes(args.stop_code)
    if not stop_codes:
        stop_codes = None

    return CollectorConfig(
        base_url=args.base_url,
        stops_csv=Path(args.stops_csv),
        output_dir=Path(args.output_dir),
        concurrency=max(1, args.concurrency),
        timeout_s=args.timeout,
        retries=max(0, args.retries),
        interval_s=max(0.0, args.interval),
        limit=args.limit,
        stop_codes=stop_codes,
        shuffle=args.shuffle,
        clickhouse_enabled=not args.no_clickhouse,
        clickhouse_url=args.clickhouse_url,
        clickhouse_database=args.clickhouse_database,
        clickhouse_user=args.clickhouse_user,
        clickhouse_password=args.clickhouse_password,
        clickhouse_batch_size=max(1, args.clickhouse_batch_size),
        clickhouse_timeout_s=max(1.0, args.clickhouse_timeout),
    )


def main(argv: list[str] | None = None) -> int:
    config = parse_args(argv)
    stops = load_stops(config.stops_csv, stop_codes=config.stop_codes, limit=config.limit)
    if not stops:
        LOG.error("No stops loaded from %s", config.stops_csv)
        return 1
    if config.shuffle:
        random.shuffle(stops)
    collect_forever(stops, config)
    return 0


if __name__ == "__main__":
    sys.exit(main())
