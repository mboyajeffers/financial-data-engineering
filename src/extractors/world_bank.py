"""
World Bank Indicators API client.

Fetches economic indicators from api.worldbank.org using
page-number pagination with metadata-driven page counts.
"""

from datetime import datetime, timezone
from typing import Dict, List, Optional

import pandas as pd

from ..base import BaseClient
from ..result import ExtractionResult


class WorldBankClient(BaseClient):
    """Client for the World Bank Indicators API.

    Demonstrates page-number pagination — the API returns
    ``[metadata, data]`` where metadata contains ``page``,
    ``pages``, ``per_page``, and ``total``.

    Usage::

        client = WorldBankClient()
        result = client.extract(
            countries=["US", "GB", "JP"],
            indicators=["NY.GDP.PCAP.CD"],
            start_year=2018,
            end_year=2023,
        )
        print(result.data.head())
    """

    source_name = "world_bank"
    base_url = "https://api.worldbank.org/v2"
    rate_limit = 60

    DEFAULT_COUNTRIES = ["US", "GB", "JP", "DE", "FR", "CA", "AU", "BR", "IN", "CN"]
    DEFAULT_INDICATORS = [
        "NY.GDP.PCAP.CD",   # GDP per capita (current US$)
        "SP.POP.TOTL",       # Total population
    ]

    def extract(
        self,
        countries: Optional[List[str]] = None,
        indicators: Optional[List[str]] = None,
        start_year: int = 2018,
        end_year: int = 2023,
        **kwargs,
    ) -> ExtractionResult:
        """Fetch economic indicators for given countries and years.

        Args:
            countries: ISO 3166-1 alpha-2 country codes.
            indicators: World Bank indicator codes.
            start_year: Start of date range.
            end_year: End of date range.

        Returns:
            ExtractionResult with a DataFrame of indicator values.
        """
        started = datetime.now(timezone.utc)
        self.reset_telemetry()

        if countries is None:
            countries = self.DEFAULT_COUNTRIES
        if indicators is None:
            indicators = self.DEFAULT_INDICATORS

        try:
            frames = []
            country_str = ";".join(countries)

            for indicator in indicators:
                df = self._fetch_indicator(country_str, indicator, start_year, end_year)
                frames.append(df)

            combined = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
            return self._build_result(combined, started)
        except Exception as exc:
            return self._build_error(str(exc), started)

    # --- Internal helpers -----------------------------------------------------

    def _fetch_indicator(
        self,
        country_str: str,
        indicator: str,
        start_year: int,
        end_year: int,
    ) -> pd.DataFrame:
        """Fetch all pages for a single indicator."""
        all_records: list = []
        page = 1

        while True:
            params = {
                "format": "json",
                "date": f"{start_year}:{end_year}",
                "per_page": 100,
                "page": page,
            }

            path = f"/country/{country_str}/indicator/{indicator}"
            raw = self._get(path, params=params)

            # World Bank returns [metadata, data]
            if not isinstance(raw, list) or len(raw) < 2:
                break

            metadata, data = raw[0], raw[1]
            if data is None:
                break

            all_records.extend(data)

            total_pages = metadata.get("pages", 1)
            if page >= total_pages:
                break
            page += 1

        return self._parse_records(all_records)

    def _parse_records(self, records: list) -> pd.DataFrame:
        """Parse World Bank JSON records into a DataFrame."""
        if not records:
            return pd.DataFrame(
                columns=[
                    "country_code", "country_name",
                    "indicator_code", "indicator_name",
                    "year", "value",
                ]
            )

        rows = []
        for rec in records:
            country = rec.get("country", {})
            indicator = rec.get("indicator", {})
            rows.append({
                "country_code": rec.get("countryiso3code", country.get("id")),
                "country_name": country.get("value"),
                "indicator_code": indicator.get("id"),
                "indicator_name": indicator.get("value"),
                "year": int(rec["date"]) if rec.get("date") else None,
                "value": rec.get("value"),
            })

        df = pd.DataFrame(rows)
        df["value"] = pd.to_numeric(df["value"], errors="coerce")
        return df
