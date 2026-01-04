"""Data transformation functions for converting API responses to storage records."""

from __future__ import annotations

import hashlib
import json
from datetime import timedelta
from pathlib import Path
from typing import Any

from roundabout.bgpp import FetchResult
from roundabout.constants import (
    COORDINATE_DECIMAL_PLACES,
    VEHICLE_KEY_HASH_LENGTH,
    VEHICLE_KEY_PREFIX_GARAGE,
    VEHICLE_KEY_PREFIX_HASH,
)
from roundabout.gtfs import Stop
from roundabout.utils import format_timestamp, parse_coords, parse_float, parse_int, round_coordinate


def normalize_vehicle(vehicle: dict[str, Any]) -> dict[str, Any]:
    """
    Normalize a vehicle record from the API response.

    Extracts and converts vehicle data into a consistent format with proper types.
    All numeric fields are parsed and None is used for missing/invalid data.

    Args:
        vehicle: Raw vehicle dictionary from API response.

    Returns:
        Normalized vehicle dictionary with typed fields.
    """
    line_number = vehicle.get("lineNumber")
    line_name = vehicle.get("lineName")
    direction = vehicle.get("direction")
    seconds_left = parse_int(vehicle.get("secondsLeft"))
    stations_between = parse_int(vehicle.get("stationsBetween"))
    vehicle_id = vehicle.get("garageNo")
    lat, lon = parse_coords(vehicle.get("coords"))

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


def build_vehicle_key(
    vehicle_id: str | None,
    line_number: str | None,
    direction: str | None,
    lat: float | None,
    lon: float | None,
    stop_code: str,
) -> str:
    """
    Generate a unique deduplication key for a vehicle.

    The key is used to track unique vehicles across different stops in a cycle.
    Two strategies are used:
    1. If vehicle_id (garage number) is available, use "garage:{vehicle_id}"
    2. Otherwise, hash the combination of line, direction, and rounded coordinates

    The hash fallback includes stop_code when coordinates are missing to avoid
    false deduplication of different vehicles on the same line.

    Args:
        vehicle_id: Garage number if available.
        line_number: Line number (route).
        direction: Direction of travel.
        lat: Vehicle latitude.
        lon: Vehicle longitude.
        stop_code: Stop code for fallback when coordinates missing.

    Returns:
        Unique vehicle key string prefixed with "garage:" or "hash:".

    Examples:
        >>> build_vehicle_key("P80276", "5", None, None, None, "1001")
        'garage:P80276'
        >>> # Hash-based key when garage number missing
        >>> build_vehicle_key(None, "5", "A", 44.79215, 20.51088, "1001")
        'hash:...'  # 16-character hash
    """
    if vehicle_id:
        return f"{VEHICLE_KEY_PREFIX_GARAGE}:{vehicle_id}"

    key_payload: dict[str, Any] = {
        "line_number": line_number,
        "direction": direction,
        "lat": round_coordinate(lat, COORDINATE_DECIMAL_PLACES),
        "lon": round_coordinate(lon, COORDINATE_DECIMAL_PLACES),
    }

    # Include stop_code when coordinates are missing to prevent false deduplication
    if key_payload["lat"] is None or key_payload["lon"] is None:
        key_payload["stop_code"] = stop_code

    raw = json.dumps(key_payload, sort_keys=True)
    digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:VEHICLE_KEY_HASH_LENGTH]
    return f"{VEHICLE_KEY_PREFIX_HASH}:{digest}"


def build_prediction_record(
    *,
    stop: Stop,
    result: FetchResult,
    vehicle: dict[str, Any],
    cycle_id: str,
) -> dict[str, Any]:
    """
    Build a prediction record for storage from API response.

    Creates a complete record of a vehicle's predicted arrival at a stop,
    including all available metadata from both the stop and vehicle.

    Args:
        stop: The stop being queried.
        result: The API fetch result.
        vehicle: Raw vehicle dictionary from API response.
        cycle_id: Unique identifier for the collection cycle.

    Returns:
        Dictionary record ready for JSON storage.
    """
    normalized = normalize_vehicle(vehicle)
    seconds_left = normalized["seconds_left"]
    observed_at = result.observed_at

    # Calculate predicted arrival time if seconds_left is available
    predicted_arrival_at = None
    if seconds_left is not None:
        predicted_arrival_at = format_timestamp(observed_at + timedelta(seconds=seconds_left))

    vehicle_key = build_vehicle_key(
        normalized["vehicle_id"],
        normalized["line_number"],
        normalized["direction"],
        normalized["vehicle_lat"],
        normalized["vehicle_lon"],
        stop.stop_code,
    )

    return {
        "observed_at": format_timestamp(observed_at),
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


def build_vehicle_record(
    *,
    stop: Stop,
    result: FetchResult,
    prediction: dict[str, Any],
) -> dict[str, Any]:
    """
    Build a deduplicated vehicle record from a prediction.

    Extracts vehicle-specific information for the unique vehicles table.
    This is called only for the first occurrence of each vehicle in a cycle.

    Args:
        stop: The stop where the vehicle was observed.
        result: The API fetch result.
        prediction: The prediction record (output of build_prediction_record).

    Returns:
        Dictionary record for the vehicles table.
    """
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


def build_error_record(
    *,
    stop: Stop,
    result: FetchResult,
    cycle_id: str,
) -> dict[str, Any]:
    """
    Build an error record from a failed API request.

    Args:
        stop: The stop that was queried.
        result: The failed fetch result.
        cycle_id: Unique identifier for the collection cycle.

    Returns:
        Dictionary record for the errors table.
    """
    return {
        "observed_at": format_timestamp(result.observed_at),
        "cycle_id": cycle_id,
        "stop_id": stop.stop_id,
        "stop_code": stop.stop_code,
        "error": result.error,
        "http_status": result.status,
        "attempts": result.attempts,
        "duration_ms": result.duration_ms,
    }


def build_output_paths(output_dir: Path, cycle_id: str, started_at) -> dict[str, Path]:
    """
    Build output file paths for a collection cycle.

    Creates paths for all output files, organized by date in YYYY/MM/DD structure.
    All files for a cycle are placed in the same date directory based on cycle start time.

    Args:
        output_dir: Base output directory.
        cycle_id: Unique cycle identifier.
        started_at: Cycle start datetime for date directory creation.

    Returns:
        Dictionary mapping file type to Path:
        - predictions: Per-stop arrival predictions
        - vehicles: Deduplicated vehicle snapshots
        - errors: Request failures
        - cycles: Cycle summary statistics
    """
    from roundabout.constants import OUTPUT_DATE_PREFIX_FORMAT

    date_prefix = started_at.strftime(OUTPUT_DATE_PREFIX_FORMAT)
    base_dir = output_dir / date_prefix

    return {
        "predictions": base_dir / f"stop_predictions_{cycle_id}.jsonl",
        "vehicles": base_dir / f"vehicles_{cycle_id}.jsonl",
        "errors": base_dir / f"errors_{cycle_id}.jsonl",
        "cycles": base_dir / "cycles.jsonl",
    }
