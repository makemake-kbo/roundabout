"""Tests for CLI argument parsing."""

from __future__ import annotations

from pathlib import Path

import pytest

from roundabout.cli import parse_args, parse_stop_codes


class TestParseStopCodes:
    """Tests for parse_stop_codes function."""

    def test_parse_single_code(self):
        result = parse_stop_codes(["1001"])
        assert result == {"1001"}

    def test_parse_multiple_codes(self):
        result = parse_stop_codes(["1001", "1002", "1003"])
        assert result == {"1001", "1002", "1003"}

    def test_parse_comma_separated(self):
        result = parse_stop_codes(["1001,1002,1003"])
        assert result == {"1001", "1002", "1003"}

    def test_parse_mixed(self):
        result = parse_stop_codes(["1001,1002", "1003"])
        assert result == {"1001", "1002", "1003"}

    def test_parse_with_whitespace(self):
        result = parse_stop_codes(["1001 , 1002 ", " 1003"])
        assert result == {"1001", "1002", "1003"}

    def test_parse_empty_list(self):
        result = parse_stop_codes([])
        assert result == set()

    def test_parse_duplicates(self):
        result = parse_stop_codes(["1001", "1001", "1002"])
        assert result == {"1001", "1002"}


class TestParseArgs:
    """Tests for parse_args function."""

    def test_parse_minimal_args(self):
        config = parse_args([])
        assert config.base_url == "https://bgpp.misa.st/api/stations/bg/search"
        assert config.stops_csv == Path("stops-data/stops.csv")
        assert config.output_dir == Path("data/raw")
        assert config.concurrency == 10
        assert config.timeout_s == 10.0
        assert config.retries == 2
        assert config.interval_s == 0.0
        assert config.limit is None
        assert config.stop_codes is None
        assert config.shuffle is False
        assert config.clickhouse_enabled is True

    def test_parse_custom_stops_csv(self):
        config = parse_args(["--stops-csv", "custom/stops.csv"])
        assert config.stops_csv == Path("custom/stops.csv")

    def test_parse_custom_output_dir(self):
        config = parse_args(["--output-dir", "output"])
        assert config.output_dir == Path("output")

    def test_parse_concurrency(self):
        config = parse_args(["--concurrency", "5"])
        assert config.concurrency == 5

    def test_parse_concurrency_minimum(self):
        config = parse_args(["--concurrency", "0"])
        assert config.concurrency == 1  # Enforced minimum

    def test_parse_timeout(self):
        config = parse_args(["--timeout", "20.5"])
        assert config.timeout_s == 20.5

    def test_parse_retries(self):
        config = parse_args(["--retries", "5"])
        assert config.retries == 5

    def test_parse_interval(self):
        config = parse_args(["--interval", "30"])
        assert config.interval_s == 30.0

    def test_parse_limit(self):
        config = parse_args(["--limit", "100"])
        assert config.limit == 100

    def test_parse_stop_codes(self):
        config = parse_args(["--stop-code", "1001", "--stop-code", "1002"])
        assert config.stop_codes == {"1001", "1002"}

    def test_parse_shuffle(self):
        config = parse_args(["--shuffle"])
        assert config.shuffle is True

    def test_parse_no_clickhouse(self):
        config = parse_args(["--no-clickhouse"])
        assert config.clickhouse_enabled is False

    def test_parse_clickhouse_url(self):
        config = parse_args(["--clickhouse-url", "http://custom:8123"])
        assert config.clickhouse_url == "http://custom:8123"

    def test_parse_clickhouse_database(self):
        config = parse_args(["--clickhouse-database", "mydb"])
        assert config.clickhouse_database == "mydb"

    def test_parse_clickhouse_batch_size(self):
        config = parse_args(["--clickhouse-batch-size", "5000"])
        assert config.clickhouse_batch_size == 5000

    def test_parse_clickhouse_batch_size_minimum(self):
        config = parse_args(["--clickhouse-batch-size", "0"])
        assert config.clickhouse_batch_size == 1  # Enforced minimum
