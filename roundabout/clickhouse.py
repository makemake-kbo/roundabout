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
    pass


@dataclass(frozen=True)
class ClickHouseConfig:
    url: str
    database: str
    user: str | None
    password: str | None
    timeout_s: float


class ClickHouseClient:
    def __init__(self, config: ClickHouseConfig) -> None:
        self._config = config
        self._settings = {
            "date_time_input_format": "best_effort",
        }

    def _table_name(self, table: str) -> str:
        if "." in table or not self._config.database:
            return table
        return f"{self._config.database}.{table}"

    def _build_url(self, query: str) -> str:
        params: dict[str, Any] = {"query": query, **self._settings}
        if self._config.user:
            params["user"] = self._config.user
        if self._config.password:
            params["password"] = self._config.password
        return f"{self._config.url.rstrip('/')}/?{urlencode(params)}"

    def insert_json_each_row(self, table: str, records: list[dict[str, Any]]) -> None:
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
    def __init__(
        self,
        client: ClickHouseClient,
        table: str,
        *,
        batch_size: int = 2000,
    ) -> None:
        self._client = client
        self._table = table
        self._batch_size = max(1, batch_size)
        self._buffer: list[dict[str, Any]] = []

    def write(self, record: dict[str, Any]) -> None:
        self._buffer.append(record)
        if len(self._buffer) >= self._batch_size:
            self.flush()

    def flush(self) -> None:
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
        self.flush()
