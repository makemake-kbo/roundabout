"""JSONL file storage writer with automatic directory creation."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, TextIO


class JsonlWriter:
    """
    Writer for JSONL (JSON Lines) format files.

    Automatically creates parent directories and supports both manual
    and context manager usage patterns.

    Each record is written as a single line of JSON, making the format
    suitable for streaming and line-by-line processing.

    Attributes:
        path: The file path being written to.

    Examples:
        >>> from pathlib import Path
        >>> # Manual usage
        >>> writer = JsonlWriter(Path("data/output.jsonl"))
        >>> writer.write({"key": "value"})
        >>> writer.close()
        >>>
        >>> # Context manager usage (recommended)
        >>> with JsonlWriter(Path("data/output.jsonl")) as writer:
        ...     writer.write({"key": "value"})
    """

    def __init__(self, path: Path) -> None:
        """
        Initialize a JSONL writer.

        Creates parent directories if they don't exist.
        Opens the file in append mode.

        Args:
            path: Output file path.
        """
        path.parent.mkdir(parents=True, exist_ok=True)
        self._path = path
        self._handle: TextIO = path.open("a", encoding="utf-8")

    @property
    def path(self) -> Path:
        """Get the output file path."""
        return self._path

    def write(self, record: dict[str, Any]) -> None:
        """
        Write a single record to the file.

        Serializes the record as JSON and appends a newline.
        Non-ASCII characters are preserved (ensure_ascii=False).

        Args:
            record: Dictionary to serialize and write.
        """
        self._handle.write(json.dumps(record, ensure_ascii=False) + "\n")

    def flush(self) -> None:
        """Flush the write buffer to disk."""
        self._handle.flush()

    def close(self) -> None:
        """Close the file handle."""
        self._handle.close()

    def __enter__(self) -> JsonlWriter:
        """Enter context manager."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Exit context manager, ensuring file is closed."""
        self.close()
