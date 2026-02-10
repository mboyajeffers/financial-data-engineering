"""
Open-Meteo Historical Weather API client.

Fetches historical weather data from archive.open-meteo.com using
array-style responses (parallel time-series arrays).
"""

from datetime import datetime, timezone
from typing import List, Optional, Tuple

import pandas as pd

from .base_client import BaseClient
from .result import ExtractionResult


class OpenMeteoClient(BaseClient):
    """Client for the Open-Meteo Historical Weather API.

    Demonstrates array-style response parsing — the API returns
    ``daily: {time: [...], temperature_2m_max: [...], ...}`` where
    each key holds a parallel array indexed by date.

    Usage::

        client = OpenMeteoClient()
        locations = [
            (40.71, -74.01, "New York"),
            (34.05, -118.24, "Los Angeles"),
        ]
        result = client.extract(
            locations=locations,
            start_date="2024-01-01",
            end_date="2024-12-31",
        )
        print(result.data.head())
    """

    source_name = "open_meteo"
    base_url = "https://archive-api.open-meteo.com/v1"
    rate_limit = 60

    DEFAULT_VARIABLES = [
        "temperature_2m_max",
        "temperature_2m_min",
        "precipitation_sum",
        "wind_speed_10m_max",
    ]

    def extract(
        self,
        locations: Optional[List[Tuple[float, float, str]]] = None,
        start_date: str = "2024-01-01",
        end_date: str = "2024-12-31",
        variables: Optional[List[str]] = None,
        **kwargs,
    ) -> ExtractionResult:
        """Fetch historical weather for one or more locations.

        Args:
            locations: List of (latitude, longitude, name) tuples.
                Defaults to a small set of world capitals.
            start_date: ISO date string for range start.
            end_date: ISO date string for range end.
            variables: Daily weather variables to request.

        Returns:
            ExtractionResult with a DataFrame of daily weather records.
        """
        started = datetime.now(timezone.utc)
        self.reset_telemetry()

        if locations is None:
            locations = [
                (40.71, -74.01, "New York"),
                (51.51, -0.13, "London"),
                (35.68, 139.69, "Tokyo"),
            ]
        if variables is None:
            variables = self.DEFAULT_VARIABLES

        try:
            frames = []
            for lat, lon, name in locations:
                df = self._fetch_location(lat, lon, name, start_date, end_date, variables)
                frames.append(df)

            combined = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
            return self._build_result(combined, started)
        except Exception as exc:
            return self._build_error(str(exc), started)

    # --- Internal helpers -----------------------------------------------------

    def _fetch_location(
        self,
        lat: float,
        lon: float,
        name: str,
        start_date: str,
        end_date: str,
        variables: List[str],
    ) -> pd.DataFrame:
        """Fetch weather data for a single location."""
        params = {
            "latitude": lat,
            "longitude": lon,
            "start_date": start_date,
            "end_date": end_date,
            "daily": ",".join(variables),
            "timezone": "UTC",
        }

        data = self._get("/archive", params=params)
        daily = data.get("daily", {})

        if not daily or "time" not in daily:
            return pd.DataFrame()

        # Build DataFrame from parallel arrays
        records = {"location": name, "date": daily["time"]}

        column_map = {
            "temperature_2m_max": "temperature_max",
            "temperature_2m_min": "temperature_min",
            "precipitation_sum": "precipitation",
            "wind_speed_10m_max": "wind_speed_max",
        }

        for var in variables:
            col_name = column_map.get(var, var)
            records[col_name] = daily.get(var, [None] * len(daily["time"]))

        df = pd.DataFrame(records)
        df["date"] = pd.to_datetime(df["date"])
        return df
