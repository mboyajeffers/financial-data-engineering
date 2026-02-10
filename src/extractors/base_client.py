"""
Base API client with built-in rate limiting, caching, and retries.

All source-specific clients inherit from BaseClient, which provides:
- Token bucket rate limiter (thread-safe, configurable requests/minute)
- Response cache (in-memory, MD5 keys, configurable TTL)
- Retry with exponential backoff and jitter (skips 4xx)
- HTTP 429 Retry-After handling
- Session pooling with custom User-Agent
- Per-request telemetry

Author: Mboya Jeffers
"""

import hashlib
import json
import logging
import random
import time
import threading
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple

import requests

from .result import ExtractionResult


class BaseClient(ABC):
    """Abstract base class for API clients.

    Subclasses implement ``source_name``, ``base_url``, ``rate_limit``,
    and ``extract()`` to pull data from a specific API.  Rate limiting,
    caching, retries, and telemetry are handled automatically.

    Usage::

        class MyClient(BaseClient):
            source_name = "my_api"
            base_url = "https://api.example.com"
            rate_limit = 60  # requests per minute

            def extract(self, **kwargs):
                data = self._get("/endpoint", params={"q": "test"})
                df = pd.DataFrame(data)
                return self._build_result(df)
    """

    # --- Abstract interface ---------------------------------------------------

    @property
    @abstractmethod
    def source_name(self) -> str:
        """Short identifier for this data source (e.g. 'usgs')."""

    @property
    @abstractmethod
    def base_url(self) -> str:
        """Root URL for this API (no trailing slash)."""

    @property
    @abstractmethod
    def rate_limit(self) -> int:
        """Maximum requests per minute for this source."""

    @abstractmethod
    def extract(self, **kwargs) -> ExtractionResult:
        """Run the extraction and return an ExtractionResult."""

    # --- Lifecycle ------------------------------------------------------------

    def __init__(self, cache_ttl: int = 300):
        """Initialize the client.

        Args:
            cache_ttl: Cache time-to-live in seconds (default 5 min).
        """
        self._cache_ttl = cache_ttl

        # Session pooling
        self._session = requests.Session()
        self._session.headers.update({
            "User-Agent": f"financial-data-engineering/{self.source_name}",
            "Accept": "application/json",
        })

        # Token bucket rate limiter
        self._tokens = float(self.rate_limit)
        self._max_tokens = float(self.rate_limit)
        self._refill_rate = self.rate_limit / 60.0  # tokens per second
        self._last_refill = time.monotonic()
        self._lock = threading.Lock()

        # Response cache: key -> (response_json, expiry_timestamp)
        self._cache: Dict[str, Tuple[Any, float]] = {}

        # Telemetry counters
        self.api_calls = 0
        self.cache_hits = 0
        self.errors = 0
        self._timings: list = []

        # Logger
        self._log = logging.getLogger(f"extractor.{self.source_name}")

    # --- Rate limiter ---------------------------------------------------------

    def _wait_for_token(self) -> None:
        """Block until a rate-limit token is available."""
        while True:
            with self._lock:
                now = time.monotonic()
                elapsed = now - self._last_refill
                self._tokens = min(
                    self._max_tokens,
                    self._tokens + elapsed * self._refill_rate,
                )
                self._last_refill = now

                if self._tokens >= 1.0:
                    self._tokens -= 1.0
                    return

            # No token available — sleep briefly and retry
            time.sleep(0.05)

    # --- Cache ----------------------------------------------------------------

    def _cache_key(self, url: str, params: Optional[Dict] = None) -> str:
        """Deterministic MD5 cache key from URL + sorted params."""
        normalized = json.dumps(params or {}, sort_keys=True)
        raw = f"{url}|{normalized}"
        return hashlib.md5(raw.encode()).hexdigest()

    def _cache_get(self, key: str) -> Optional[Any]:
        """Return cached value if present and not expired."""
        entry = self._cache.get(key)
        if entry is None:
            return None
        value, expiry = entry
        if time.time() > expiry:
            del self._cache[key]
            return None
        return value

    def _cache_set(self, key: str, value: Any) -> None:
        """Store a value in the cache with TTL."""
        self._cache[key] = (value, time.time() + self._cache_ttl)

    # --- HTTP with retries ----------------------------------------------------

    def _get(
        self,
        path: str,
        params: Optional[Dict] = None,
        max_retries: int = 3,
        use_cache: bool = True,
    ) -> Any:
        """GET request with rate limiting, caching, and retries.

        Args:
            path: URL path appended to ``base_url``.
            params: Query parameters.
            max_retries: Max retry attempts on transient errors.
            use_cache: Whether to check/store in cache.

        Returns:
            Parsed JSON response.

        Raises:
            requests.HTTPError: On non-retryable HTTP errors.
        """
        url = f"{self.base_url}{path}" if path.startswith("/") else path

        # Check cache first
        if use_cache:
            key = self._cache_key(url, params)
            cached = self._cache_get(key)
            if cached is not None:
                self.cache_hits += 1
                self._log.debug("Cache hit: %s", key[:8])
                return cached

        # Retry loop
        last_error = None
        for attempt in range(max_retries + 1):
            self._wait_for_token()
            self.api_calls += 1
            start = time.monotonic()

            try:
                resp = self._session.get(url, params=params, timeout=30)
                elapsed = time.monotonic() - start
                self._timings.append(elapsed)

                # 429 Too Many Requests — honour Retry-After
                if resp.status_code == 429:
                    retry_after = int(resp.headers.get("Retry-After", 5))
                    self._log.warning(
                        "Rate limited (429). Retry-After: %ds", retry_after
                    )
                    time.sleep(retry_after)
                    continue

                # 4xx (except 429) — don't retry
                if 400 <= resp.status_code < 500:
                    self.errors += 1
                    resp.raise_for_status()

                # 5xx — retry with backoff
                if resp.status_code >= 500:
                    wait = (2 ** attempt) + random.uniform(0, 1)
                    self._log.warning(
                        "Server error %d, retry %d/%d in %.1fs",
                        resp.status_code, attempt + 1, max_retries, wait,
                    )
                    last_error = requests.HTTPError(
                        f"{resp.status_code}", response=resp
                    )
                    time.sleep(wait)
                    continue

                # Success
                data = resp.json()
                if use_cache:
                    self._cache_set(key, data)
                return data

            except requests.ConnectionError as exc:
                elapsed = time.monotonic() - start
                self._timings.append(elapsed)
                self.errors += 1
                last_error = exc
                wait = (2 ** attempt) + random.uniform(0, 1)
                self._log.warning(
                    "Connection error, retry %d/%d in %.1fs",
                    attempt + 1, max_retries, wait,
                )
                if attempt < max_retries:
                    time.sleep(wait)

        # Exhausted retries
        self.errors += 1
        raise last_error  # type: ignore[misc]

    # --- Result builder -------------------------------------------------------

    def _build_result(
        self,
        data,
        started_at: datetime,
        warnings: Optional[list] = None,
    ) -> ExtractionResult:
        """Build a successful ExtractionResult from a DataFrame."""
        import pandas as pd

        completed = datetime.now(timezone.utc)
        records = len(data) if isinstance(data, pd.DataFrame) else 0
        return ExtractionResult(
            success=True,
            source=self.source_name,
            records=records,
            api_calls=self.api_calls,
            cache_hits=self.cache_hits,
            started_at=started_at,
            completed_at=completed,
            duration_seconds=(completed - started_at).total_seconds(),
            warnings=warnings or [],
            data=data,
        )

    def _build_error(
        self, error: str, started_at: datetime
    ) -> ExtractionResult:
        """Build a failed ExtractionResult."""
        completed = datetime.now(timezone.utc)
        return ExtractionResult(
            success=False,
            source=self.source_name,
            records=0,
            api_calls=self.api_calls,
            cache_hits=self.cache_hits,
            started_at=started_at,
            completed_at=completed,
            duration_seconds=(completed - started_at).total_seconds(),
            error=error,
        )

    # --- Telemetry ------------------------------------------------------------

    def get_telemetry(self) -> Dict[str, Any]:
        """Return telemetry summary for this client."""
        return {
            "source": self.source_name,
            "api_calls": self.api_calls,
            "cache_hits": self.cache_hits,
            "errors": self.errors,
            "avg_latency": (
                sum(self._timings) / len(self._timings)
                if self._timings
                else 0.0
            ),
        }

    def reset_telemetry(self) -> None:
        """Reset all telemetry counters."""
        self.api_calls = 0
        self.cache_hits = 0
        self.errors = 0
        self._timings.clear()
