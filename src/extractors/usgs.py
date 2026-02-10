"""
USGS Earthquake Hazards API client.

Fetches seismic event data from earthquake.usgs.gov using
offset-based pagination over GeoJSON responses.
"""

from datetime import datetime, timezone
from typing import Optional

import pandas as pd

from ..base import BaseClient
from ..result import ExtractionResult


class USGSClient(BaseClient):
    """Client for the USGS Earthquake Hazards Program API.

    Demonstrates offset-based pagination — each request uses ``limit``
    and ``offset`` query parameters to page through large result sets.

    Usage::

        client = USGSClient()
        result = client.extract(
            start_date="2025-01-01",
            end_date="2025-12-31",
            min_magnitude=5.0,
        )
        print(result.data.head())
    """

    source_name = "usgs"
    base_url = "https://earthquake.usgs.gov/fdsnws/event/1"
    rate_limit = 60

    PAGE_SIZE = 500

    def extract(
        self,
        start_date: str = "2025-01-01",
        end_date: str = "2025-12-31",
        min_magnitude: float = 4.5,
        max_results: int = 2000,
        **kwargs,
    ) -> ExtractionResult:
        """Fetch earthquakes within the given parameters.

        Args:
            start_date: ISO date string for range start.
            end_date: ISO date string for range end.
            min_magnitude: Minimum magnitude filter.
            max_results: Cap on total records returned.

        Returns:
            ExtractionResult with a DataFrame of earthquake events.
        """
        started = datetime.now(timezone.utc)
        self.reset_telemetry()

        try:
            records = self._paginate(
                start_date, end_date, min_magnitude, max_results
            )
            df = self._to_dataframe(records)
            return self._build_result(df, started)
        except Exception as exc:
            return self._build_error(str(exc), started)

    # --- Internal helpers -----------------------------------------------------

    def _paginate(
        self,
        start_date: str,
        end_date: str,
        min_magnitude: float,
        max_results: int,
    ) -> list:
        """Offset pagination: fetch pages until exhausted or cap hit."""
        all_features: list = []
        offset = 1  # USGS offset is 1-based

        while len(all_features) < max_results:
            limit = min(self.PAGE_SIZE, max_results - len(all_features))
            params = {
                "format": "geojson",
                "starttime": start_date,
                "endtime": end_date,
                "minmagnitude": min_magnitude,
                "limit": limit,
                "offset": offset,
                "orderby": "magnitude",
            }

            data = self._get("/query", params=params)
            features = data.get("features", [])

            if not features:
                break

            all_features.extend(features)
            offset += len(features)

            # If we got fewer than requested, we've exhausted the data
            if len(features) < limit:
                break

        return all_features

    def _to_dataframe(self, features: list) -> pd.DataFrame:
        """Parse GeoJSON features into a flat DataFrame."""
        if not features:
            return pd.DataFrame(
                columns=[
                    "id", "magnitude", "place", "time",
                    "latitude", "longitude", "depth", "type", "status",
                ]
            )

        rows = []
        for f in features:
            props = f.get("properties", {})
            coords = f.get("geometry", {}).get("coordinates", [None, None, None])
            rows.append({
                "id": f.get("id"),
                "magnitude": props.get("mag"),
                "place": props.get("place"),
                "time": pd.to_datetime(props.get("time"), unit="ms", utc=True),
                "latitude": coords[1] if len(coords) > 1 else None,
                "longitude": coords[0] if coords else None,
                "depth": coords[2] if len(coords) > 2 else None,
                "type": props.get("type"),
                "status": props.get("status"),
            })

        return pd.DataFrame(rows)
