"""Shared test fixtures and path setup."""
import sys
from pathlib import Path

# Add project root to sys.path so tests can import src.*
sys.path.insert(0, str(Path(__file__).parent.parent))

import pytest
import pandas as pd


# --- Extraction fixtures (USGS, Open-Meteo, World Bank) ---

@pytest.fixture
def mock_geojson():
    """Sample USGS GeoJSON response with 3 earthquake features."""
    return {
        "type": "FeatureCollection",
        "metadata": {"generated": 1700000000000, "count": 3},
        "features": [
            {
                "id": "us7000l1aa",
                "type": "Feature",
                "properties": {
                    "mag": 7.1,
                    "place": "100 km S of Honshu, Japan",
                    "time": 1700000000000,
                    "type": "earthquake",
                    "status": "reviewed",
                },
                "geometry": {"type": "Point", "coordinates": [139.69, 35.68, 30.0]},
            },
            {
                "id": "us7000l1bb",
                "type": "Feature",
                "properties": {
                    "mag": 5.5,
                    "place": "50 km NE of Los Angeles, CA",
                    "time": 1700010000000,
                    "type": "earthquake",
                    "status": "automatic",
                },
                "geometry": {"type": "Point", "coordinates": [-118.24, 34.05, 12.5]},
            },
            {
                "id": "us7000l1cc",
                "type": "Feature",
                "properties": {
                    "mag": 4.8,
                    "place": "20 km W of Lima, Peru",
                    "time": 1700020000000,
                    "type": "earthquake",
                    "status": "reviewed",
                },
                "geometry": {"type": "Point", "coordinates": [-77.04, -12.05, 45.0]},
            },
        ],
    }


@pytest.fixture
def mock_weather():
    """Sample Open-Meteo daily weather response."""
    return {
        "latitude": 40.71,
        "longitude": -74.01,
        "daily": {
            "time": ["2024-01-01", "2024-01-02", "2024-01-03"],
            "temperature_2m_max": [5.2, 3.8, 7.1],
            "temperature_2m_min": [-1.0, -2.5, 0.3],
            "precipitation_sum": [0.0, 12.5, 2.1],
            "wind_speed_10m_max": [15.3, 22.1, 8.7],
        },
    }


@pytest.fixture
def mock_worldbank():
    """Sample World Bank paginated response (page 1 of 1)."""
    return [
        {"page": 1, "pages": 1, "per_page": 100, "total": 3},
        [
            {"indicator": {"id": "NY.GDP.PCAP.CD", "value": "GDP per capita"}, "country": {"id": "US", "value": "United States"}, "countryiso3code": "USA", "date": "2023", "value": 80034.567},
            {"indicator": {"id": "NY.GDP.PCAP.CD", "value": "GDP per capita"}, "country": {"id": "GB", "value": "United Kingdom"}, "countryiso3code": "GBR", "date": "2023", "value": 48913.234},
            {"indicator": {"id": "NY.GDP.PCAP.CD", "value": "GDP per capita"}, "country": {"id": "JP", "value": "Japan"}, "countryiso3code": "JPN", "date": "2023", "value": 33950.789},
        ],
    ]


@pytest.fixture
def sample_earthquakes_df():
    """Pre-built earthquake DataFrame for merge tests."""
    return pd.DataFrame({
        "id": ["eq1", "eq2", "eq3"],
        "magnitude": [7.1, 5.5, 4.8],
        "place": ["Japan", "USA", "Peru"],
        "country_code": ["JPN", "USA", "PER"],
    })


@pytest.fixture
def sample_weather_df():
    """Pre-built weather DataFrame for merge tests."""
    return pd.DataFrame({
        "location": ["Tokyo", "New York", "Lima"],
        "country_code": ["JPN", "USA", "PER"],
        "temperature_max": [15.3, 5.2, 28.1],
    })


@pytest.fixture
def sample_indicators_df():
    """Pre-built indicators DataFrame for merge tests."""
    return pd.DataFrame({
        "country_code": ["USA", "GBR", "JPN"],
        "country_name": ["United States", "United Kingdom", "Japan"],
        "indicator_code": ["NY.GDP.PCAP.CD"] * 3,
        "year": [2023, 2023, 2023],
        "value": [80034.567, 48913.234, 33950.789],
    })


# --- Quality validation fixtures ---

@pytest.fixture
def clean_df():
    """DataFrame with no quality issues."""
    return pd.DataFrame({
        'id': [1, 2, 3, 4, 5],
        'name': ['Alpha', 'Bravo', 'Charlie', 'Delta', 'Echo'],
        'score': [85, 92, 78, 95, 88],
        'email': ['a@test.com', 'b@test.com', 'c@test.com', 'd@test.com', 'e@test.com'],
    })


@pytest.fixture
def messy_df():
    """DataFrame with completeness, uniqueness, and range issues."""
    return pd.DataFrame({
        'id': [1, 2, 2, 4, None],
        'name': ['Alpha', 'Bravo', 'Bravo', '', 'Echo'],
        'score': [85, 120, 78, -5, 90],
        'email': ['a@test.com', 'bad-email', 'c@test.com', 'd@test.com', None],
    })


@pytest.fixture
def financial_df():
    """Realistic financial data for validation."""
    return pd.DataFrame({
        'cik': ['0000320193', '0000789019', '0001018724', '0001652044', '0001326801'],
        'company_name': ['Apple Inc', 'Microsoft Corp', 'Amazon.com Inc', 'Alphabet Inc', 'Meta Platforms'],
        'ticker': ['AAPL', 'MSFT', 'AMZN', 'GOOGL', 'META'],
        'revenue': [394_328_000_000, 211_915_000_000, 513_983_000_000, 282_836_000_000, 116_609_000_000],
        'net_income': [99_803_000_000, 72_738_000_000, 30_425_000_000, 59_972_000_000, 39_098_000_000],
    })


@pytest.fixture
def sample_records():
    """Sample financial records for testing."""
    return [
        {"date": "2025-01-15", "symbol": "AAPL", "close": 198.50, "volume": 45_000_000},
        {"date": "2025-01-15", "symbol": "MSFT", "close": 412.30, "volume": 22_000_000},
    ]
