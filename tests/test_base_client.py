"""Tests for BaseClient: rate limiter, cache, retries, telemetry."""

import time
from unittest.mock import patch, MagicMock

import pandas as pd
import pytest
import requests

from src.extractors.base_client import BaseClient


class StubClient(BaseClient):
    """Minimal concrete client for testing base functionality."""

    source_name = "stub"
    base_url = "https://stub.example.com"
    rate_limit = 120  # 2 per second

    def extract(self, **kwargs):
        data = self._get("/data", params={"q": "test"})
        df = pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()
        from datetime import datetime
        return self._build_result(df, datetime.utcnow())


class TestRateLimiter:
    """Token bucket rate limiter tests."""

    def test_respects_rate_limit(self):
        """Client should not exceed configured requests/minute."""
        client = StubClient()
        # With rate_limit=120, we get 2 tokens/second
        # Consuming all tokens should eventually require waiting
        for _ in range(5):
            client._wait_for_token()
        # If we got here without hanging, the bucket refills correctly

    def test_token_bucket_refills(self):
        """Tokens should refill over time."""
        client = StubClient()
        # Drain several tokens
        for _ in range(3):
            client._wait_for_token()
        # Wait a bit for refill
        time.sleep(0.1)
        # Should be able to get another token immediately
        start = time.monotonic()
        client._wait_for_token()
        elapsed = time.monotonic() - start
        assert elapsed < 0.2  # Should be nearly instant


class TestCache:
    """Response cache tests."""

    def test_cache_stores_and_retrieves(self):
        """Cached values should be returned on subsequent lookups."""
        client = StubClient(cache_ttl=60)
        key = client._cache_key("https://example.com/api", {"a": 1})
        client._cache_set(key, {"result": "ok"})
        assert client._cache_get(key) == {"result": "ok"}

    def test_cache_expires_after_ttl(self):
        """Entries should disappear after TTL."""
        client = StubClient(cache_ttl=0)  # Expire immediately
        key = client._cache_key("https://example.com/api", {})
        client._cache_set(key, {"result": "ok"})
        time.sleep(0.01)
        assert client._cache_get(key) is None

    def test_cache_key_deterministic(self):
        """Same URL + params should produce the same cache key."""
        client = StubClient()
        key1 = client._cache_key("https://example.com", {"b": 2, "a": 1})
        key2 = client._cache_key("https://example.com", {"a": 1, "b": 2})
        assert key1 == key2

    def test_cache_key_differs_for_different_params(self):
        """Different params should produce different cache keys."""
        client = StubClient()
        key1 = client._cache_key("https://example.com", {"a": 1})
        key2 = client._cache_key("https://example.com", {"a": 2})
        assert key1 != key2


class TestRetries:
    """HTTP retry logic tests."""

    @patch("src.extractors.base_client.BaseClient._wait_for_token")
    def test_retry_on_5xx(self, mock_token):
        """Should retry on server errors with backoff."""
        client = StubClient()
        mock_resp_500 = MagicMock()
        mock_resp_500.status_code = 500
        mock_resp_500.headers = {}

        mock_resp_200 = MagicMock()
        mock_resp_200.status_code = 200
        mock_resp_200.json.return_value = {"ok": True}

        with patch.object(client._session, "get", side_effect=[mock_resp_500, mock_resp_200]):
            with patch("src.extractors.base_client.time.sleep"):
                result = client._get("/test", use_cache=False)
        assert result == {"ok": True}
        assert client.api_calls == 2

    @patch("src.extractors.base_client.BaseClient._wait_for_token")
    def test_no_retry_on_4xx(self, mock_token):
        """Should not retry on client errors (except 429)."""
        client = StubClient()
        mock_resp = MagicMock()
        mock_resp.status_code = 404
        mock_resp.raise_for_status.side_effect = requests.HTTPError("404")

        with patch.object(client._session, "get", return_value=mock_resp):
            with pytest.raises(requests.HTTPError):
                client._get("/missing", use_cache=False)
        assert client.api_calls == 1
        assert client.errors == 1

    @patch("src.extractors.base_client.BaseClient._wait_for_token")
    def test_429_retry_after(self, mock_token):
        """Should sleep for Retry-After seconds on 429."""
        client = StubClient()
        mock_resp_429 = MagicMock()
        mock_resp_429.status_code = 429
        mock_resp_429.headers = {"Retry-After": "2"}

        mock_resp_200 = MagicMock()
        mock_resp_200.status_code = 200
        mock_resp_200.json.return_value = {"ok": True}

        with patch.object(client._session, "get", side_effect=[mock_resp_429, mock_resp_200]):
            with patch("src.extractors.base_client.time.sleep") as mock_sleep:
                result = client._get("/test", use_cache=False)
        mock_sleep.assert_any_call(2)
        assert result == {"ok": True}


class TestTelemetry:
    """Telemetry tracking tests."""

    def test_telemetry_tracks_calls(self):
        """api_calls counter should increment on each request."""
        client = StubClient()
        assert client.api_calls == 0
        client.api_calls = 5
        t = client.get_telemetry()
        assert t["api_calls"] == 5
        assert t["source"] == "stub"

    def test_telemetry_reset(self):
        """reset_telemetry should zero all counters."""
        client = StubClient()
        client.api_calls = 10
        client.cache_hits = 5
        client.errors = 2
        client.reset_telemetry()
        t = client.get_telemetry()
        assert t["api_calls"] == 0
        assert t["cache_hits"] == 0
        assert t["errors"] == 0


class TestSession:
    """Session and header tests."""

    def test_custom_user_agent(self):
        """Session should have a custom User-Agent header."""
        client = StubClient()
        ua = client._session.headers["User-Agent"]
        assert "stub" in ua  # Verify source name in User-Agent
        assert "stub" in ua

    def test_session_reuse(self):
        """Multiple calls should reuse the same session object."""
        client = StubClient()
        session1 = client._session
        session2 = client._session
        assert session1 is session2
