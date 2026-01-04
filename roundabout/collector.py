"""
Main collector module - orchestrates data collection from BG++ API.

This module re-exports the main entry point and provides backward compatibility
with the original monolithic collector structure.
"""

from __future__ import annotations

import logging
import random
import sys

from roundabout.cli import parse_args
from roundabout.gtfs import load_stops
from roundabout.orchestrator import collect_forever

LOG = logging.getLogger(__name__)


def main(argv: list[str] | None = None) -> int:
    """
    Main entry point for the roundabout collector.

    Parses command-line arguments, loads stops data, and begins collection.

    Args:
        argv: Command-line arguments (default: sys.argv).

    Returns:
        Exit code: 0 for success, 1 for error.

    Examples:
        >>> # Run single collection cycle
        >>> main(["--limit", "10"])
        >>> # Run continuously every 30 seconds
        >>> main(["--interval", "30"])
    """
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
