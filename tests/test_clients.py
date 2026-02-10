"""Tests for USGS, Open-Meteo, and World Bank clients."""

from unittest.mock import patch, MagicMock
from datetime import datetime

import pandas as pd
import pytest

from collector.clients.usgs import USGSClient
from collector.clients.open_meteo import OpenMeteoClient
from collector.clients.world_bank import WorldBankClient


class TestUSGSClient:
    """USGS Earthquake API client tests."""

    @patch.object(USGSClient, "_get")
    def test_geojson_parsing(self, mock_get, mock_geojson):
        """Should parse GeoJSON features into a flat DataFrame."""
        mock_get.return_value = mock_geojson
        client = USGSClient()
        result = client.extract(start_date="2024-01-01", end_date="2024-12-31")

        assert result.success
        assert result.records == 3
        assert "magnitude" in result.data.columns
        assert "latitude" in result.data.columns
        assert result.data["magnitude"].iloc[0] == 7.1

    @patch.object(USGSClient, "_get")
    def test_offset_pagination(self, mock_get, mock_geojson):
        """Should paginate until fewer results than page size."""
        # First call returns 3, second returns empty (exhausted)
        empty = {"type": "FeatureCollection", "features": []}
        mock_get.side_effect = [mock_geojson, empty]
        client = USGSClient()
        result = client.extract(max_results=5000)

        assert result.success
        assert result.records == 3
        assert mock_get.call_count == 1  # Only 1 page needed (3 < 500)

    @patch.object(USGSClient, "_get")
    def test_empty_response(self, mock_get):
        """Should handle empty feature sets gracefully."""
        mock_get.return_value = {"type": "FeatureCollection", "features": []}
        client = USGSClient()
        result = client.extract()

        assert result.success
        assert result.records == 0
        assert len(result.data) == 0

    @patch.object(USGSClient, "_get")
    def test_error_handling(self, mock_get):
        """Should return error result on exception."""
        mock_get.side_effect = Exception("Connection timeout")
        client = USGSClient()
        result = client.extract()

        assert not result.success
        assert "Connection timeout" in result.error


class TestOpenMeteoClient:
    """Open-Meteo Historical Weather API client tests."""

    @patch.object(OpenMeteoClient, "_get")
    def test_array_to_dataframe(self, mock_get, mock_weather):
        """Should convert parallel arrays into rows."""
        mock_get.return_value = mock_weather
        client = OpenMeteoClient()
        result = client.extract(
            locations=[(40.71, -74.01, "New York")],
            start_date="2024-01-01",
            end_date="2024-01-03",
        )

        assert result.success
        assert result.records == 3
        assert "temperature_max" in result.data.columns
        assert "precipitation" in result.data.columns

    @patch.object(OpenMeteoClient, "_get")
    def test_multi_location(self, mock_get, mock_weather):
        """Should concatenate results from multiple locations."""
        mock_get.return_value = mock_weather  # Same data for each location
        client = OpenMeteoClient()
        locations = [
            (40.71, -74.01, "New York"),
            (34.05, -118.24, "Los Angeles"),
        ]
        result = client.extract(locations=locations)

        assert result.success
        assert result.records == 6  # 3 days * 2 locations
        assert mock_get.call_count == 2

    @patch.object(OpenMeteoClient, "_get")
    def test_missing_daily_data(self, mock_get):
        """Should handle missing daily key gracefully."""
        mock_get.return_value = {"latitude": 40.71, "longitude": -74.01}
        client = OpenMeteoClient()
        result = client.extract(locations=[(40.71, -74.01, "New York")])

        assert result.success
        assert result.records == 0

    @patch.object(OpenMeteoClient, "_get")
    def test_error_handling(self, mock_get):
        """Should return error result on exception."""
        mock_get.side_effect = Exception("API unavailable")
        client = OpenMeteoClient()
        result = client.extract()

        assert not result.success
        assert "API unavailable" in result.error


class TestWorldBankClient:
    """World Bank Indicators API client tests."""

    @patch.object(WorldBankClient, "_get")
    def test_page_metadata_parsing(self, mock_get, mock_worldbank):
        """Should parse [metadata, data] response format."""
        mock_get.return_value = mock_worldbank
        client = WorldBankClient()
        result = client.extract(
            countries=["US", "GB", "JP"],
            indicators=["NY.GDP.PCAP.CD"],
        )

        assert result.success
        assert result.records == 3
        assert "country_code" in result.data.columns
        assert "value" in result.data.columns

    @patch.object(WorldBankClient, "_get")
    def test_multi_indicator(self, mock_get, mock_worldbank):
        """Should fetch each indicator separately and combine."""
        mock_get.return_value = mock_worldbank
        client = WorldBankClient()
        result = client.extract(
            countries=["US"],
            indicators=["NY.GDP.PCAP.CD", "SP.POP.TOTL"],
        )

        assert result.success
        assert mock_get.call_count == 2  # One call per indicator

    @patch.object(WorldBankClient, "_get")
    def test_empty_indicator(self, mock_get):
        """Should handle null data array gracefully."""
        mock_get.return_value = [{"page": 1, "pages": 1, "total": 0}, None]
        client = WorldBankClient()
        result = client.extract(indicators=["FAKE.IND"])

        assert result.success
        assert result.records == 0

    @patch.object(WorldBankClient, "_get")
    def test_error_handling(self, mock_get):
        """Should return error result on exception."""
        mock_get.side_effect = Exception("Rate limited")
        client = WorldBankClient()
        result = client.extract()

        assert not result.success
        assert "Rate limited" in result.error
