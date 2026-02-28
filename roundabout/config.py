"""Configuration data structures for the roundabout collector."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class CollectorConfig:
    """
    Configuration for the data collector.

    This configuration controls all aspects of data collection including API access,
    concurrency, output locations, route filtering, rate limiting, vehicle tracking,
    and database connectivity.

    Attributes:
        base_url: Base URL for the BG++ API endpoint.
        stops_csv: Path to the GTFS stops.csv file.
        routes_csv: Path to the GTFS routes.csv file.
        trips_csv: Path to the GTFS trips.csv file.
        stop_times_csv: Path to the GTFS stop_times CSV file(s).
        output_dir: Directory where JSONL output files will be written.
        concurrency: Maximum number of concurrent API requests.
        timeout_s: HTTP request timeout in seconds.
        retries: Number of retry attempts for failed requests.
        interval_s: Delay between collection cycles in seconds (0 for single run).
        limit: Maximum number of stops to process (None for all).
        stop_codes: Set of specific stop codes to process (None for all).
        route_short_names: Set of route short names to process (None for all).
        shuffle: Whether to randomize stop processing order.
        jsonl_enabled: Whether to write data to JSONL files.
        clickhouse_enabled: Whether to write data to ClickHouse.
        clickhouse_url: ClickHouse HTTP interface URL.
        clickhouse_database: ClickHouse database name.
        clickhouse_user: ClickHouse username (None for no auth).
        clickhouse_password: ClickHouse password (None for no auth).
        clickhouse_batch_size: Number of records to batch before writing.
        clickhouse_timeout_s: ClickHouse request timeout in seconds.
        bbox_min_lat: Minimum latitude for geographic bounding box (None for no filter).
        bbox_max_lat: Maximum latitude for geographic bounding box (None for no filter).
        bbox_min_lon: Minimum longitude for geographic bounding box (None for no filter).
        bbox_max_lon: Maximum longitude for geographic bounding box (None for no filter).
        rate_limit_rps: Rate limit in requests per second.
        rate_limit_enabled: Whether rate limiting is enabled.
        vehicle_tracking_enabled: Whether cross-cycle vehicle tracking is enabled.
        vehicle_tracking_ttl_cycles: Number of cycles to track vehicles before cleanup.
    """

    base_url: str
    stops_csv: Path
    routes_csv: Path
    trips_csv: Path
    stop_times_csv: Path
    output_dir: Path
    concurrency: int
    timeout_s: float
    retries: int
    interval_s: float
    limit: int | None
    stop_codes: set[str] | None
    route_short_names: set[str] | None
    shuffle: bool
    jsonl_enabled: bool
    clickhouse_enabled: bool
    clickhouse_url: str
    clickhouse_database: str
    clickhouse_user: str | None
    clickhouse_password: str | None
    clickhouse_batch_size: int
    clickhouse_timeout_s: float
    bbox_min_lat: float | None
    bbox_max_lat: float | None
    bbox_min_lon: float | None
    bbox_max_lon: float | None
    rate_limit_rps: float
    rate_limit_enabled: bool
    vehicle_tracking_enabled: bool
    vehicle_tracking_ttl_cycles: int
    optimistic_enabled: bool
    calendar_csv: Path
    calendar_dates_csv: Path
    checkpoint_stride: int
    verification_batch_size: int
    discovery_interval_cycles: int
    deviation_threshold_s: int
    stuck_threshold_cycles: int
    lost_threshold_cycles: int


@dataclass(frozen=True)
class CycleSummary:
    """
    Summary statistics for a single collection cycle.

    A cycle represents one complete pass through all configured stops,
    fetching arrival predictions and vehicle data from the API.

    Attributes:
        cycle_id: Unique identifier for the cycle (timestamp-based).
        started_at: When the cycle began.
        finished_at: When the cycle completed.
        stops_total: Total number of stops processed.
        responses: Number of successful API responses.
        errors: Number of failed API requests.
        predictions: Total number of vehicle predictions collected.
        unique_vehicles: Number of unique vehicles seen in this cycle.
    """

    cycle_id: str
    started_at: datetime
    finished_at: datetime
    stops_total: int
    responses: int
    errors: int
    predictions: int
    unique_vehicles: int
    stops_polled: int | None = None
    stops_predicted: int | None = None
    vehicles_verified: int | None = None
    vehicles_delayed: int | None = None
    vehicles_stuck: int | None = None

    def as_record(self) -> dict[str, Any]:
        """
        Convert the summary to a dictionary record for storage.

        Returns:
            Dictionary with all fields formatted for JSON serialization.
        """
        from roundabout.utils import format_timestamp

        record = {
            "cycle_id": self.cycle_id,
            "started_at": format_timestamp(self.started_at),
            "finished_at": format_timestamp(self.finished_at),
            "stops_total": self.stops_total,
            "responses": self.responses,
            "errors": self.errors,
            "predictions": self.predictions,
            "unique_vehicles": self.unique_vehicles,
        }
        if self.stops_polled is not None:
            record["stops_polled"] = self.stops_polled
        if self.stops_predicted is not None:
            record["stops_predicted"] = self.stops_predicted
        if self.vehicles_verified is not None:
            record["vehicles_verified"] = self.vehicles_verified
        if self.vehicles_delayed is not None:
            record["vehicles_delayed"] = self.vehicles_delayed
        if self.vehicles_stuck is not None:
            record["vehicles_stuck"] = self.vehicles_stuck
        return record
