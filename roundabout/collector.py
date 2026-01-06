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

    # Load all stops first
    all_stops = load_stops(config.stops_csv)
    if not all_stops:
        LOG.error("No stops loaded from %s", config.stops_csv)
        return 1

    LOG.info("Loaded %d stops from %s", len(all_stops), config.stops_csv)

    # Apply geographic bounding box filter if configured
    if all(
        [
            config.bbox_min_lat is not None,
            config.bbox_max_lat is not None,
            config.bbox_min_lon is not None,
            config.bbox_max_lon is not None,
        ]
    ):
        from roundabout.gtfs import filter_stops_by_bbox

        all_stops = filter_stops_by_bbox(
            all_stops,
            config.bbox_min_lat,
            config.bbox_max_lat,
            config.bbox_min_lon,
            config.bbox_max_lon,
        )
        LOG.info("Filtered to %d stops within bounding box", len(all_stops))

    # Filter by route if route names specified
    if config.route_short_names:
        from roundabout.gtfs import build_route_stops_mapping, load_routes, load_trips

        # Load routes
        routes = load_routes(
            config.routes_csv,
            route_short_names=config.route_short_names,
        )
        if not routes:
            LOG.error("No routes found matching: %s", config.route_short_names)
            return 1

        LOG.info("Loaded %d priority routes", len(routes))

        # Load trips for these routes
        route_ids = {r.route_id for r in routes}
        trips = load_trips(config.trips_csv, route_ids=route_ids)
        LOG.info("Loaded %d trips for priority routes", len(trips))

        # Build mapping of route -> stops
        route_stops = build_route_stops_mapping(
            routes,
            trips,
            config.stop_times_csv,
            all_stops,
        )

        # Log stops per route
        for route_name, route_stop_list in route_stops.items():
            LOG.info("Route %s: %d stops", route_name, len(route_stop_list))

        # Deduplicate stops across all priority routes
        unique_stops_set = set()
        for route_stop_list in route_stops.values():
            unique_stops_set.update(route_stop_list)

        stops = list(unique_stops_set)
        LOG.info(
            "Total unique stops across %d routes: %d", len(routes), len(stops)
        )
    else:
        # Fall back to all stops (or filtered by stop_codes/limit)
        stops = all_stops
        if config.stop_codes:
            stops = [s for s in stops if s.stop_code in config.stop_codes]
            LOG.info("Filtered to %d stops by stop codes", len(stops))
        if config.limit:
            stops = stops[: config.limit]
            LOG.info("Limited to %d stops", len(stops))

    if not stops:
        LOG.error("No stops to collect after filtering")
        return 1

    LOG.info("Starting collection for %d stops", len(stops))

    if config.shuffle:
        random.shuffle(stops)

    collect_forever(stops, config)
    return 0


if __name__ == "__main__":
    sys.exit(main())
