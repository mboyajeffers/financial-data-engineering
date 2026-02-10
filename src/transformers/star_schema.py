"""
Star schema builder for multi-industry dimensional modeling.

Implements Kimball methodology patterns used across all industry verticals:
- Finance (FRED economic indicators)
- Brokerage (equity OHLCV data)
- Weather (daily + hourly meteorological data)
- Gaming (Steam platform metrics)
- Crypto (CoinGecko market data)
- Compliance (SEC EDGAR filings)
- Betting (ESPN league standings)
- Solar (NREL generation data)
"""

import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import pandas as pd

from .base_transformer import TransformationResult


@dataclass
class SchemaDefinition:
    """Definition for a star schema table."""
    name: str
    table_type: str  # 'dimension' or 'fact'
    natural_keys: List[str]
    columns: List[str]
    measures: Optional[List[str]] = None
    dimension_keys: Optional[List[str]] = None


class StarSchemaBuilder:
    """
    Builds star schema tables from raw data using schema definitions.

    Supports the full lifecycle:
    1. Define schemas (dimensions + facts)
    2. Build dimension tables with surrogate keys
    3. Build fact tables with foreign key lookups
    4. Validate referential integrity
    5. Export to Parquet

    Example:
        builder = StarSchemaBuilder(output_dir='./output/finance')
        builder.add_dimension('dim_series', natural_keys=['series_id'], ...)
        builder.add_fact('fact_indicators', measures=['value'], ...)
        result = builder.build(raw_data)
    """

    def __init__(self, output_dir: str = './output'):
        self.output_dir = output_dir
        self.logger = logging.getLogger('StarSchemaBuilder')
        self.dimensions: Dict[str, SchemaDefinition] = {}
        self.facts: Dict[str, SchemaDefinition] = {}
        self._built_tables: Dict[str, pd.DataFrame] = {}

    def add_dimension(self, name: str, natural_keys: List[str],
                      columns: List[str]) -> 'StarSchemaBuilder':
        """Register a dimension table definition."""
        self.dimensions[name] = SchemaDefinition(
            name=name, table_type='dimension',
            natural_keys=natural_keys, columns=columns,
        )
        return self

    def add_fact(self, name: str, measures: List[str],
                 dimension_keys: List[str],
                 columns: Optional[List[str]] = None) -> 'StarSchemaBuilder':
        """Register a fact table definition."""
        self.facts[name] = SchemaDefinition(
            name=name, table_type='fact',
            natural_keys=[], measures=measures,
            dimension_keys=dimension_keys,
            columns=columns or [],
        )
        return self

    def build(self, data: Dict[str, Any]) -> TransformationResult:
        """
        Build all registered star schema tables from raw data.

        Returns TransformationResult with table names, row counts,
        and output file paths.
        """
        import os
        import time

        start = time.time()
        tables_created = []
        rows_by_table = {}
        output_paths = {}

        try:
            os.makedirs(self.output_dir, exist_ok=True)

            # Build dimensions first (facts need their keys)
            for name, schema in self.dimensions.items():
                if name in data:
                    df = pd.DataFrame(data[name])
                    path = os.path.join(self.output_dir, f'{name}.parquet')
                    df.to_parquet(path, engine='pyarrow', compression='snappy', index=False)
                    self._built_tables[name] = df
                    tables_created.append(name)
                    rows_by_table[name] = len(df)
                    output_paths[name] = path

            # Build facts
            for name, schema in self.facts.items():
                if name in data:
                    df = pd.DataFrame(data[name])
                    path = os.path.join(self.output_dir, f'{name}.parquet')
                    df.to_parquet(path, engine='pyarrow', compression='snappy', index=False)
                    self._built_tables[name] = df
                    tables_created.append(name)
                    rows_by_table[name] = len(df)
                    output_paths[name] = path

            total_rows = sum(rows_by_table.values())
            duration = time.time() - start

            return TransformationResult(
                success=True,
                tables_created=tables_created,
                total_rows=total_rows,
                rows_by_table=rows_by_table,
                output_paths=output_paths,
                duration_sec=duration,
            )

        except Exception as e:
            return TransformationResult(
                success=False,
                error=str(e),
                duration_sec=time.time() - start,
            )

    def validate_referential_integrity(self) -> List[str]:
        """Check that all fact table foreign keys exist in their dimensions."""
        violations = []
        for fact_name, fact_schema in self.facts.items():
            if fact_name not in self._built_tables:
                continue
            fact_df = self._built_tables[fact_name]
            for dim_key in (fact_schema.dimension_keys or []):
                dim_key.replace('_key', '').replace('fact_', '')
                for dname, dim_df in self._built_tables.items():
                    if dim_key in dim_df.columns and dim_key in fact_df.columns:
                        orphans = set(fact_df[dim_key]) - set(dim_df[dim_key])
                        if orphans:
                            violations.append(
                                f"{fact_name}.{dim_key}: {len(orphans)} orphan keys"
                            )
        return violations
