from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Iterator


@dataclass(frozen=True)
class Stop:
    stop_id: int
    stop_code: str
    stop_name: str
    stop_lat: float
    stop_lon: float


@dataclass(frozen=True)
class StopTime:
    trip_id: str
    arrival_time: str
    departure_time: str
    stop_id: int
    stop_sequence: int
    pickup_type: int | None
    drop_off_type: int | None
    timepoint: int | None


def _parse_float(value: str | None) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _parse_int(value: str | None) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def iter_stops(stops_csv: Path) -> Iterator[Stop]:
    with stops_csv.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            stop_code = (row.get("stop_code") or "").strip()
            if not stop_code:
                continue
            stop_id_raw = (row.get("stop_id") or "").strip()
            if not stop_id_raw:
                continue
            try:
                stop_id = int(stop_id_raw)
            except ValueError:
                continue
            stop_name = (row.get("stop_name") or "").strip()
            stop_lat = _parse_float(row.get("stop_lat"))
            stop_lon = _parse_float(row.get("stop_lon"))
            if stop_lat is None or stop_lon is None:
                continue
            yield Stop(
                stop_id=stop_id,
                stop_code=stop_code,
                stop_name=stop_name,
                stop_lat=stop_lat,
                stop_lon=stop_lon,
            )


def load_stops(
    stops_csv: Path,
    *,
    stop_codes: set[str] | None = None,
    limit: int | None = None,
) -> list[Stop]:
    stops: list[Stop] = []
    for stop in iter_stops(stops_csv):
        if stop_codes is not None and stop.stop_code not in stop_codes:
            continue
        stops.append(stop)
        if limit is not None and len(stops) >= limit:
            break
    return stops


def resolve_stop_times_files(stop_times_path: Path) -> list[Path]:
    if stop_times_path.is_dir():
        return sorted(stop_times_path.glob("stop_times*.csv"))
    if stop_times_path.exists():
        return [stop_times_path]
    return sorted(stop_times_path.parent.glob("stop_times_*.csv"))


def iter_stop_times(stop_times_path: Path) -> Iterator[StopTime]:
    paths = resolve_stop_times_files(stop_times_path)
    for path in paths:
        with path.open("r", newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                trip_id = (row.get("trip_id") or "").strip()
                arrival_time = (row.get("arrival_time") or "").strip()
                departure_time = (row.get("departure_time") or "").strip()
                stop_id = _parse_int((row.get("stop_id") or "").strip())
                stop_sequence = _parse_int((row.get("stop_sequence") or "").strip())
                if not trip_id or stop_id is None or stop_sequence is None:
                    continue
                yield StopTime(
                    trip_id=trip_id,
                    arrival_time=arrival_time,
                    departure_time=departure_time,
                    stop_id=stop_id,
                    stop_sequence=stop_sequence,
                    pickup_type=_parse_int((row.get("pickup_type") or "").strip() or None),
                    drop_off_type=_parse_int((row.get("drop_off_type") or "").strip() or None),
                    timepoint=_parse_int((row.get("timepoint") or "").strip() or None),
                )
