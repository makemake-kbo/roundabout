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
    concurrency, output locations, and database connectivity.

    Attributes:
        base_url: Base URL for the BG++ API endpoint.
        stops_csv: Path to the GTFS stops.csv file.
        output_dir: Directory where JSONL output files will be written.
        concurrency: Maximum number of concurrent API requests.
        timeout_s: HTTP request timeout in seconds.
        retries: Number of retry attempts for failed requests.
        interval_s: Delay between collection cycles in seconds (0 for single run).
        limit: Maximum number of stops to process (None for all).
        stop_codes: Set of specific stop codes to process (None for all).
        shuffle: Whether to randomize stop processing order.
        clickhouse_enabled: Whether to write data to ClickHouse.
        clickhouse_url: ClickHouse HTTP interface URL.
        clickhouse_database: ClickHouse database name.
        clickhouse_user: ClickHouse username (None for no auth).
        clickhouse_password: ClickHouse password (None for no auth).
        clickhouse_batch_size: Number of records to batch before writing.
        clickhouse_timeout_s: ClickHouse request timeout in seconds.
    """

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

    def as_record(self) -> dict[str, Any]:
        """
        Convert the summary to a dictionary record for storage.

        Returns:
            Dictionary with all fields formatted for JSON serialization.
        """
        from roundabout.utils import format_timestamp

        return {
            "cycle_id": self.cycle_id,
            "started_at": format_timestamp(self.started_at),
            "finished_at": format_timestamp(self.finished_at),
            "stops_total": self.stops_total,
            "responses": self.responses,
            "errors": self.errors,
            "predictions": self.predictions,
            "unique_vehicles": self.unique_vehicles,
        }
