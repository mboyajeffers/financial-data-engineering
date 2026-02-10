"""
Extraction result container.

Structured output from any API extraction, with telemetry
and optional DataFrame payload.
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd


@dataclass
class ExtractionResult:
    """Result of a single extraction operation.

    Carries the extracted data, telemetry counters, and any
    errors or warnings produced during extraction.
    """

    success: bool
    source: str
    records: int = 0
    api_calls: int = 0
    cache_hits: int = 0
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    duration_seconds: float = 0.0
    error: Optional[str] = None
    warnings: List[str] = field(default_factory=list)
    data: Optional[pd.DataFrame] = None

    def to_dict(self) -> Dict[str, Any]:
        """Serialize to a JSON-safe dictionary (excludes DataFrame)."""
        return {
            "success": self.success,
            "source": self.source,
            "records": self.records,
            "api_calls": self.api_calls,
            "cache_hits": self.cache_hits,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "duration_seconds": self.duration_seconds,
            "error": self.error,
            "warnings": self.warnings,
        }
