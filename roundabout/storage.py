from __future__ import annotations

import json
from pathlib import Path
from typing import Any, TextIO


class JsonlWriter:
    def __init__(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self._path = path
        self._handle: TextIO = path.open("a", encoding="utf-8")

    @property
    def path(self) -> Path:
        return self._path

    def write(self, record: dict[str, Any]) -> None:
        self._handle.write(json.dumps(record, ensure_ascii=False) + "\n")

    def flush(self) -> None:
        self._handle.flush()

    def close(self) -> None:
        self._handle.close()
