"""Tests for MultiSourceCollector orchestrator."""

from unittest.mock import patch, MagicMock
from datetime import datetime

import pandas as pd
import pytest

from collector.collector import MultiSourceCollector
from collector.result import ExtractionResult
from collector.clients.usgs import USGSClient
from collector.clients.open_meteo import OpenMeteoClient


class TestRegistration:
    """Client registration and listing tests."""

    def test_register_and_list(self):
        """Should track registered sources by name."""
        c = MultiSourceCollector()
        c.register("usgs", USGSClient())
        c.register("weather", OpenMeteoClient())
        assert sorted(c.list_sources()) == ["usgs", "weather"]

    def test_empty_collector(self):
        """New collector should have no sources."""
        c = MultiSourceCollector()
        assert c.list_sources() == []


class TestCollect:
    """Single and multi-source collection tests."""

    def test_collect_single_source(self):
        """Should extract from a single named source."""
        c = MultiSourceCollector()
        client = MagicMock()
        client.extract.return_value = ExtractionResult(
            success=True, source="mock", records=10
        )
        c.register("mock", client)
        result = c.collect("mock", min_magnitude=5.0)

        assert result.success
        assert result.records == 10
        client.extract.assert_called_once_with(min_magnitude=5.0)

    def test_collect_unknown_source(self):
        """Should raise KeyError for unregistered source."""
        c = MultiSourceCollector()
        with pytest.raises(KeyError, match="not registered"):
            c.collect("nonexistent")

    def test_collect_all(self):
        """Should extract from all registered sources."""
        c = MultiSourceCollector()
        for name in ["src_a", "src_b"]:
            client = MagicMock()
            client.extract.return_value = ExtractionResult(
                success=True, source=name, records=5
            )
            c.register(name, client)

        results = c.collect_all()
        assert len(results) == 2
        assert all(r.success for r in results.values())

    def test_error_isolation(self):
        """One failing source should not block others."""
        c = MultiSourceCollector()
        good = MagicMock()
        good.extract.return_value = ExtractionResult(
            success=True, source="good", records=10
        )
        bad = MagicMock()
        bad.extract.side_effect = RuntimeError("API down")

        c.register("good", good)
        c.register("bad", bad)
        results = c.collect_all()

        assert results["good"].success
        assert not results["bad"].success
        assert "API down" in results["bad"].error

    def test_empty_collection(self):
        """collect_all with no sources should return empty dict."""
        c = MultiSourceCollector()
        results = c.collect_all()
        assert results == {}


class TestTelemetry:
    """Telemetry aggregation tests."""

    def test_aggregation(self):
        """Should sum telemetry across all clients."""
        c = MultiSourceCollector()
        for name in ["a", "b"]:
            client = MagicMock()
            client.get_telemetry.return_value = {
                "source": name, "api_calls": 3, "cache_hits": 1, "errors": 0,
            }
            c.register(name, client)

        t = c.get_telemetry()
        assert t["totals"]["api_calls"] == 6
        assert t["totals"]["cache_hits"] == 2
        assert len(t["per_source"]) == 2


class TestMerge:
    """DataFrame merge helper tests."""

    def test_merge_results(self, sample_earthquakes_df, sample_weather_df):
        """Should join two DataFrames on a shared key."""
        merged = MultiSourceCollector.merge_results(
            sample_earthquakes_df,
            sample_weather_df,
            left_on="country_code",
            right_on="country_code",
        )
        assert len(merged) == 3
        assert "magnitude" in merged.columns
        assert "temperature_max" in merged.columns


class TestExtractionResult:
    """ExtractionResult serialization tests."""

    def test_to_dict(self):
        """to_dict should produce JSON-safe output without DataFrame."""
        r = ExtractionResult(
            success=True,
            source="test",
            records=5,
            api_calls=2,
            cache_hits=1,
            started_at=datetime(2024, 1, 1, 12, 0, 0),
            completed_at=datetime(2024, 1, 1, 12, 0, 5),
            duration_seconds=5.0,
            data=pd.DataFrame({"a": [1, 2, 3]}),
        )
        d = r.to_dict()
        assert d["success"] is True
        assert d["records"] == 5
        assert "data" not in d  # DataFrame excluded
        assert d["started_at"] == "2024-01-01T12:00:00"
