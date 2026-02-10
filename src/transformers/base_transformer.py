"""
Base transformer for Kimball star schema dimensional modeling.

Provides the foundation for transforming raw API data into
dimension and fact tables with surrogate keys.
"""

import hashlib
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd


@dataclass
class TransformationResult:
    """Result of a transformation operation."""
    success: bool
    tables_created: List[str] = field(default_factory=list)
    total_rows: int = 0
    rows_by_table: Dict[str, int] = field(default_factory=dict)
    output_paths: Dict[str, str] = field(default_factory=dict)
    error: Optional[str] = None
    duration_sec: float = 0


class BaseTransformer(ABC):
    """
    Abstract base transformer for star schema modeling.

    Subclasses implement transform() to convert raw extracted data
    into dimension and fact tables following Kimball methodology:

    - Dimension tables: descriptive attributes (dim_*)
    - Fact tables: numeric measures with foreign keys to dimensions (fact_*)
    - Surrogate keys: hash-based, deterministic, idempotent
    - Date dimension: standardized calendar attributes
    """

    def __init__(self, output_dir: str = './output', **kwargs):
        self.output_dir = output_dir
        self.logger = logging.getLogger(self.__class__.__name__)
        self._tables: Dict[str, pd.DataFrame] = {}

    @abstractmethod
    def transform(self, raw_data: Dict[str, Any]) -> TransformationResult:
        """Transform raw extracted data into star schema tables."""
        pass

    def generate_surrogate_key(self, *args) -> str:
        """Generate deterministic surrogate key from natural key components."""
        key_input = '|'.join(str(a) for a in args)
        return hashlib.md5(key_input.encode()).hexdigest()[:12]

    def generate_date_key(self, date_str: str) -> int:
        """Generate integer date key (YYYYMMDD format)."""
        dt = datetime.strptime(str(date_str)[:10], '%Y-%m-%d')
        return int(dt.strftime('%Y%m%d'))

    def build_date_dimension(self, dates: List[str]) -> pd.DataFrame:
        """Build a standard calendar dimension from a list of date strings."""
        unique_dates = sorted(set(str(d)[:10] for d in dates))
        records = []
        for date_str in unique_dates:
            dt = datetime.strptime(date_str, '%Y-%m-%d')
            records.append({
                'date_key': self.generate_date_key(date_str),
                'date': date_str,
                'year': dt.year,
                'quarter': (dt.month - 1) // 3 + 1,
                'month': dt.month,
                'month_name': dt.strftime('%B'),
                'day_of_week': dt.strftime('%A'),
                'day_of_year': dt.timetuple().tm_yday,
                'is_weekend': dt.weekday() >= 5,
            })
        return pd.DataFrame(records)

    def save_table(self, name: str, df: pd.DataFrame, path: str) -> str:
        """Save a DataFrame as Parquet with snappy compression."""
        import os
        os.makedirs(os.path.dirname(path) if os.path.dirname(path) else '.', exist_ok=True)
        df.to_parquet(path, engine='pyarrow', compression='snappy', index=False)
        self._tables[name] = df
        self.logger.info(f"Saved {name}: {len(df):,} rows -> {path}")
        return path

    def get_all_tables(self) -> Dict[str, pd.DataFrame]:
        """Return all generated tables."""
        return self._tables
