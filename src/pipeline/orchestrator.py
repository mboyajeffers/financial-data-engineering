"""
Multi-source collector orchestrator.

Registers multiple API clients, runs extractions, and
aggregates telemetry across all sources.
"""

from typing import Any, Dict, List, Optional

import pandas as pd

from .base import BaseClient
from .result import ExtractionResult


class MultiSourceCollector:
    """Orchestrate extractions from multiple API clients.

    Usage::

        from collector import MultiSourceCollector, USGSClient, WorldBankClient

        c = MultiSourceCollector()
        c.register("usgs", USGSClient())
        c.register("world_bank", WorldBankClient())

        results = c.collect_all(usgs={"min_magnitude": 5.0})
        for name, result in results.items():
            print(f"{name}: {result.records} records")
    """

    def __init__(self):
        self._clients: Dict[str, BaseClient] = {}

    def register(self, name: str, client: BaseClient) -> None:
        """Register a client under a given name."""
        self._clients[name] = client

    def list_sources(self) -> List[str]:
        """Return the names of all registered sources."""
        return list(self._clients.keys())

    def collect(self, name: str, **kwargs) -> ExtractionResult:
        """Run extraction for a single registered source.

        Args:
            name: Registered source name.
            **kwargs: Passed to the client's ``extract()`` method.

        Returns:
            ExtractionResult from the source.

        Raises:
            KeyError: If the source is not registered.
        """
        if name not in self._clients:
            raise KeyError(f"Source '{name}' is not registered")
        return self._clients[name].extract(**kwargs)

    def collect_all(self, **source_kwargs) -> Dict[str, ExtractionResult]:
        """Run extraction for all registered sources.

        Args:
            **source_kwargs: Per-source keyword arguments.
                Keys are source names; values are dicts passed to
                each source's ``extract()`` method.

        Returns:
            Dict mapping source name to ExtractionResult.
            Sources that fail return an error result without
            blocking other sources.
        """
        results: Dict[str, ExtractionResult] = {}

        for name, client in self._clients.items():
            kwargs = source_kwargs.get(name, {})
            if not isinstance(kwargs, dict):
                kwargs = {}
            try:
                results[name] = client.extract(**kwargs)
            except Exception as exc:
                results[name] = ExtractionResult(
                    success=False,
                    source=name,
                    error=str(exc),
                )

        return results

    def get_telemetry(self) -> Dict[str, Any]:
        """Aggregate telemetry across all registered clients."""
        per_source = {}
        totals = {"api_calls": 0, "cache_hits": 0, "errors": 0}

        for name, client in self._clients.items():
            t = client.get_telemetry()
            per_source[name] = t
            totals["api_calls"] += t["api_calls"]
            totals["cache_hits"] += t["cache_hits"]
            totals["errors"] += t["errors"]

        return {"totals": totals, "per_source": per_source}

    @staticmethod
    def merge_results(
        left: pd.DataFrame,
        right: pd.DataFrame,
        left_on: str,
        right_on: str,
        how: str = "inner",
    ) -> pd.DataFrame:
        """Merge two DataFrames from different extraction results.

        Args:
            left: First DataFrame.
            right: Second DataFrame.
            left_on: Column name in ``left`` to join on.
            right_on: Column name in ``right`` to join on.
            how: Join type (default 'inner').

        Returns:
            Merged DataFrame.
        """
        return pd.merge(left, right, left_on=left_on, right_on=right_on, how=how)
