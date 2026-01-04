"""ClickHouse HTTP client for batch insertions."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

LOG = logging.getLogger(__name__)


class ClickHouseError(RuntimeError):
    """Exception raised for ClickHouse API errors."""

    pass


@dataclass(frozen=True)
class ClickHouseConfig:
    """
    Configuration for ClickHouse HTTP client.

    Attributes:
        url: ClickHouse HTTP interface URL (e.g., http://localhost:8123).
        database: Default database name for table resolution.
        user: Username for authentication (None for no auth).
        password: Password for authentication (None for no auth).
        timeout_s: Request timeout in seconds.
    """

    url: str
    database: str
    user: str | None
    password: str | None
    timeout_s: float


class ClickHouseClient:
    """
    HTTP client for ClickHouse database operations.

    Supports JSON insertion via the JSONEachRow format. Handles authentication
    and automatic database prefixing for table names.

    Examples:
        >>> from roundabout.clickhouse import ClickHouseClient, ClickHouseConfig
        >>> config = ClickHouseConfig(
        ...     url="http://localhost:8123",
        ...     database="roundabout",
        ...     user="default",
        ...     password=None,
        ...     timeout_s=10.0,
        ... )
        >>> client = ClickHouseClient(config)
        >>> records = [{"id": 1, "name": "test"}]
        >>> client.insert_json_each_row("my_table", records)
    """
    def __init__(self, config: ClickHouseConfig) -> None:
        """
        Initialize ClickHouse client.

        Args:
            config: Client configuration.
        """
        self._config = config
        self._settings = {
            "date_time_input_format": "best_effort",
        }

    def _table_name(self, table: str) -> str:
        """
        Resolve fully-qualified table name.

        If table already contains a dot or no database is configured,
        returns table as-is. Otherwise prefixes with database name.

        Args:
            table: Table name (may include database prefix).

        Returns:
            Fully-qualified table name.
        """
        if "." in table or not self._config.database:
            return table
        return f"{self._config.database}.{table}"

    def _build_url(self, query: str) -> str:
        """
        Build ClickHouse HTTP API URL with query and authentication.

        Args:
            query: SQL query to execute.

        Returns:
            Complete URL with query parameters.
        """
        params: dict[str, Any] = {"query": query, **self._settings}
        if self._config.user:
            params["user"] = self._config.user
        if self._config.password:
            params["password"] = self._config.password
        return f"{self._config.url.rstrip('/')}/?{urlencode(params)}"

    def execute(self, query: str) -> dict[str, Any]:
        """
        Execute a SQL query and return results.

        For INSERT queries, returns metadata about rows written.
        For SELECT queries, would need additional handling (not implemented).

        Args:
            query: SQL query to execute.

        Returns:
            Dictionary with query results or metadata.

        Raises:
            ClickHouseError: On HTTP errors or connection failures.
        """
        url = self._build_url(query)
        request = Request(url, method="POST")
        try:
            with urlopen(request, timeout=self._config.timeout_s) as response:
                body = response.read().decode("utf-8")
                # For INSERT queries, ClickHouse returns summary info in headers
                rows_written = response.headers.get("X-ClickHouse-Summary", "")
                return {"rows_written": self._parse_rows_written(rows_written), "body": body}
        except HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            raise ClickHouseError(f"http_error:{exc.code}:{body}") from exc
        except URLError as exc:
            raise ClickHouseError(f"url_error:{exc.reason}") from exc

    def _parse_rows_written(self, summary: str) -> int:
        """
        Parse rows written from ClickHouse summary header.

        Args:
            summary: X-ClickHouse-Summary header value.

        Returns:
            Number of rows written, or 0 if cannot parse.
        """
        try:
            data = json.loads(summary)
            return data.get("written_rows", 0)
        except (json.JSONDecodeError, ValueError):
            return 0

    def insert_json_each_row(self, table: str, records: list[dict[str, Any]]) -> None:
        """
        Insert records into ClickHouse using JSONEachRow format.

        Each record is serialized as JSON on a separate line. Empty record
        lists are ignored (no-op).

        Args:
            table: Table name (auto-prefixed with database if needed).
            records: List of dictionaries to insert.

        Raises:
            ClickHouseError: On HTTP errors or connection failures.
        """
        if not records:
            return
        table_name = self._table_name(table)
        query = f"INSERT INTO {table_name} FORMAT JSONEachRow"
        url = self._build_url(query)
        payload = "\n".join(json.dumps(record, ensure_ascii=False) for record in records) + "\n"
        data = payload.encode("utf-8")
        request = Request(url, data=data, method="POST")
        request.add_header("Content-Type", "application/json; charset=utf-8")
        try:
            with urlopen(request, timeout=self._config.timeout_s) as response:
                response.read()
        except HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            raise ClickHouseError(f"http_error:{exc.code}:{body}") from exc
        except URLError as exc:
            raise ClickHouseError(f"url_error:{exc.reason}") from exc


class ClickHouseBatchWriter:
    """
    Buffered batch writer for ClickHouse insertions.

    Accumulates records in memory and flushes to ClickHouse when the batch
    size is reached. Errors during flush are logged but don't raise exceptions,
    allowing collection to continue even if the database is unavailable.

    Examples:
        >>> client = ClickHouseClient(config)
        >>> writer = ClickHouseBatchWriter(client, "my_table", batch_size=1000)
        >>> for record in records:
        ...     writer.write(record)  # Auto-flushes at batch_size
        >>> writer.close()  # Flush remaining records
    """

    def __init__(
        self,
        client: ClickHouseClient,
        table: str,
        *,
        batch_size: int = 2000,
    ) -> None:
        """
        Initialize batch writer.

        Args:
            client: ClickHouse client for insertions.
            table: Target table name.
            batch_size: Number of records to buffer before flushing (min: 1).
        """
        self._client = client
        self._table = table
        self._batch_size = max(1, batch_size)
        self._buffer: list[dict[str, Any]] = []

    def write(self, record: dict[str, Any]) -> None:
        """
        Write a single record, auto-flushing when batch size reached.

        Args:
            record: Dictionary to write to ClickHouse.
        """
        self._buffer.append(record)
        if len(self._buffer) >= self._batch_size:
            self.flush()

    def flush(self) -> None:
        """
        Flush buffered records to ClickHouse.

        Errors are logged but not raised, allowing collection to continue.
        The buffer is cleared regardless of success or failure.
        """
        if not self._buffer:
            return
        try:
            self._client.insert_json_each_row(self._table, self._buffer)
        except ClickHouseError as exc:
            LOG.error(
                "ClickHouse insert failed table=%s rows=%s error=%s",
                self._table,
                len(self._buffer),
                exc,
            )
        finally:
            self._buffer.clear()

    def close(self) -> None:
        """
        Flush any remaining buffered records.

        Should be called when done writing to ensure all records are persisted.
        """
        self.flush()
