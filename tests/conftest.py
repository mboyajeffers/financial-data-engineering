"""Shared test fixtures."""
import pytest


@pytest.fixture
def sample_records():
    """Sample financial records for testing."""
    return [
        {"date": "2025-01-15", "symbol": "AAPL", "close": 198.50, "volume": 45_000_000},
        {"date": "2025-01-15", "symbol": "MSFT", "close": 412.30, "volume": 22_000_000},
        {"date": "2025-01-16", "symbol": "AAPL", "close": 201.10, "volume": 52_000_000},
        {"date": "2025-01-16", "symbol": "MSFT", "close": 415.80, "volume": 25_000_000},
    ]
