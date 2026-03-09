# Financial Data Engineering

Production ETL pipelines with **3 implemented extractors**, Kimball star schema modeling, and a data quality framework with **68 tests**. Extensible to any REST API.

[![CI](https://github.com/mboyajeffers/financial-data-engineering/actions/workflows/ci.yml/badge.svg)](https://github.com/mboyajeffers/financial-data-engineering/actions)

## What This Is

End-to-end data engineering: API extraction, dimensional modeling, data quality validation, and automated report generation. Every pipeline follows the same pattern — extract from a public API, transform into star schema tables, validate output quality, deliver as Parquet + PDF.

## Architecture

```
Public APIs ──> Extractors ──> Transformers ──> Quality Gates ──> Parquet + Reports
   (3+)         (rate-limited    (Kimball star     (6 rule types)    (columnar)
                 + cached)        schema)
```

## Scale

| Metric | Value |
|--------|-------|
| Implemented extractors | 3 (Open-Meteo, USGS, World Bank) |
| Extraction patterns | Array, offset, and page-number pagination |
| Star schema tables | dim_/fact_ pairs per pipeline |
| Data quality rules | 6 types, 68 tests |
| Extensibility | Any REST API via BaseClient |

## Implemented Data Sources

| Source | Extractor | Pagination Pattern | Auth |
|--------|-----------|-------------------|------|
| Open-Meteo | `open_meteo.py` | Array (latitude/longitude batches) | None |
| USGS | `usgs.py` | Offset-based | None |
| World Bank | `world_bank.py` | Page-number | None |

All three extractors hit free, public APIs — no API keys required. The `BaseClient` framework supports adding any REST API with rate limiting, caching, and retry logic built in.

## Pipeline Patterns

### Extraction

Every extractor inherits from `BaseClient` which provides:
- **Rate limiting** — token-bucket algorithm, thread-safe
- **Response caching** — MD5-keyed with configurable TTL
- **Retries** — exponential backoff with jitter (3 attempts)
- **Telemetry** — API calls, cache hits, duration, error isolation

```python
from src.extractors import BaseClient, ExtractionResult

class FREDClient(BaseClient):
    BASE_URL = "https://api.stlouisfed.org/fred"
    RATE_LIMIT = 120  # requests per minute

    def extract(self, series_ids, limit=5000):
        results = []
        for series_id in series_ids:
            data = self._get(f"/series/observations",
                             params={"series_id": series_id, "limit": limit})
            results.extend(data["observations"])
        return ExtractionResult(success=True, data=results,
                                records_extracted=len(results))
```

### Transformation (Kimball Star Schema)

Raw API data is modeled into dimensions and facts:

```python
from src.transformers import BaseTransformer, TransformationResult

class FinanceTransformer(BaseTransformer):
    def transform(self, raw_data):
        # Build dimension tables
        dim_series = self._build_dim_series(raw_data["series"])
        dim_date = self.build_date_dimension(raw_data["dates"])

        # Build fact table with surrogate keys
        fact_indicators = self._build_fact(raw_data["observations"],
                                            dim_series, dim_date)

        # Save as Parquet
        self.save_table("dim_series", dim_series, f"{self.output_dir}/dim_series.parquet")
        self.save_table("dim_date", dim_date, f"{self.output_dir}/dim_date.parquet")
        self.save_table("fact_economic_indicators", fact_indicators,
                        f"{self.output_dir}/fact_economic_indicators.parquet")

        return TransformationResult(success=True, total_rows=len(fact_indicators))
```

### Data Quality Validation

Six rule types validate every pipeline output:

```python
from src.quality import DataValidator, CompletenessRule, RangeRule

validator = DataValidator()
validator.add_rule(CompletenessRule("close", threshold=0.95))
validator.add_rule(RangeRule("volume", min_val=0))

report = validator.validate(fact_daily_prices)
# ValidationReport: 2 rules, 2 passed, 0 failed
```

| Rule Type | What It Checks |
|-----------|----------------|
| Completeness | Null rate below threshold |
| Uniqueness | No duplicate natural keys |
| Range | Values within expected bounds |
| Pattern | Regex match on string columns |
| Custom | Arbitrary boolean predicates |
| RuleSet | Composite rules with AND/OR logic |

## Star Schema Examples

### Finance (FRED)
```
dim_series ──┐
             ├──> fact_economic_indicators
dim_date ────┘
```
- **dim_series**: series_key, series_id, title, units, frequency
- **fact_economic_indicators**: date_key, series_key, value, extraction_id

### Brokerage (Yahoo Finance)
```
dim_security ──┐
               ├──> fact_daily_prices
dim_date ──────┘
```
- **dim_security**: security_key, symbol, name, sector, industry, market_cap
- **fact_daily_prices**: date_key, security_key, open, high, low, close, volume

### Weather (Open-Meteo)
```
dim_location ──┐
               ├──> fact_hourly_weather (2.6M rows)
dim_date ──────┘
```
- **dim_location**: location_key, city, latitude, longitude, state
- **fact_hourly_weather**: date_key, location_key, hour, temp_c, humidity_pct, precipitation_mm

## Sample Reports

The `reports/samples/` directory contains example output — automated PDF reports generated from pipeline data:

- `01_Finance_Weekly_Intelligence.pdf` — FRED macro indicators, yield curve analysis
- `10_Executive_Summary.pdf` — Cross-vertical KPI rollup
- `Monthly_01_Finance_Intelligence.pdf` — Month-over-month trend analysis

Full report set available in [financial-market-analysis](https://github.com/mboyajeffers/financial-market-analysis).

## Project Structure

```
src/
├── extractors/          # API clients with rate limiting + caching
│   ├── base_client.py   # BaseClient ABC (rate limiter, cache, retries)
│   ├── result.py        # ExtractionResult dataclass
│   ├── open_meteo.py    # Weather data (array pagination)
│   ├── usgs.py          # Earthquake data (offset pagination)
│   └── world_bank.py    # Economic data (page-number pagination)
├── transformers/        # Kimball star schema modeling
│   ├── base_transformer.py  # Surrogate keys, date dimension, Parquet export
│   └── star_schema.py       # Schema builder with integrity validation
├── quality/             # Data validation framework
│   ├── validator.py     # DataValidator orchestrator
│   ├── rules.py         # 6 rule types (completeness, uniqueness, range, ...)
│   └── report.py        # Structured validation reports
├── pipeline/            # Orchestration
│   └── orchestrator.py  # Multi-source collector with error isolation
examples/                # Runnable examples against live APIs
tests/                   # 68 tests across all modules
reports/samples/         # Sample PDF output
```

## Running

```bash
# Install
pip install -r requirements.txt

# Test
pytest tests/ -v

# Lint
ruff check src/ tests/

# Run an example (hits live APIs — no keys needed)
python examples/collect_earthquakes.py
python examples/multi_source_pipeline.py
```

## ML Integration

The extraction and transformation patterns in this repo support downstream ML workflows:

- **Feature engineering** — star schema fact tables serve as feature stores for model training
- **Time-series modeling** — extractors pull historical data suitable for GARCH, momentum classification, and anomaly detection
- **Walk-forward backtesting** — pipeline orchestrator supports sequential data windows for unbiased model evaluation

ML models built on this data foundation: see [Data-Engineering-Portfolio](https://github.com/mboyajeffers/Data-Engineering-Portfolio).

## Tech Stack

| Layer | Tools |
|-------|-------|
| Extraction | Python, requests, token-bucket rate limiting |
| Transformation | pandas, Kimball star schema |
| Storage | Parquet (pyarrow, snappy compression) |
| Quality | Custom validation framework (6 rule types) |
| Reports | WeasyPrint PDF generation |
| Infrastructure | GCP Compute Engine, PostgreSQL, Terraform |
| CI/CD | GitHub Actions, ruff, pytest |
| Containerization | Docker, docker-compose |

## Author

**Mboya Jeffers** — Data & ML Engineer

- [GitHub](https://github.com/mboyajeffers)
- [LinkedIn](https://linkedin.com/in/mboyajeffers)
