# Financial Data Engineering

Production ETL pipelines processing **4.3M+ rows** across 8 industries using public APIs and Kimball star schema modeling.

[![CI](https://github.com/mboyajeffers/financial-data-engineering/actions/workflows/ci.yml/badge.svg)](https://github.com/mboyajeffers/financial-data-engineering/actions)

## What This Is

End-to-end data engineering: API extraction, dimensional modeling, data quality validation, and automated report generation. Every pipeline follows the same pattern — extract from a public API, transform into star schema tables, validate output quality, deliver as Parquet + PDF.

## Architecture

```
Public APIs ──> Extractors ──> Transformers ──> Quality Gates ──> Parquet + Reports
   (8)          (rate-limited    (Kimball star     (6 rule types)    (4.3M rows)
                 + cached)        schema)
```

## Scale

| Metric | Value |
|--------|-------|
| Total rows | 4.3M+ |
| Industry verticals | 8 |
| Data sources | 8 public APIs |
| Star schema tables | 30+ (dim_/fact_) |
| Report cadences | Weekly, Monthly, Quarterly |
| Branded reports | 23 PDFs |

## Data Sources

| Source | API | Data | Rows |
|--------|-----|------|------|
| Open-Meteo | ERA5 Archive | Hourly weather, 30 US cities, 10 years | 2.6M |
| SEC EDGAR | XBRL | Corporate filings, financial facts | 570K |
| Yahoo Finance | Market Data | Equity OHLCV, 200 tickers, 5 years | 529K |
| FRED | St. Louis Fed | 50 macro series (GDP, rates, labor) | 368K |
| Open-Meteo | Forecast | Daily weather, 30 cities | 109K |
| Steam + SteamSpy | Gaming | Player counts, ownership, revenue | 37K |
| ESPN | Sports | Standings across 4 leagues | 21K |
| CoinGecko | Crypto | Market caps, volumes, prices | 21K |

No API keys required for Open-Meteo, Yahoo, Steam, ESPN, or CoinGecko. FRED requires a free key from [fred.stlouisfed.org](https://fred.stlouisfed.org/docs/api/api_key.html).

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

Full report set: 9 weekly + 9 monthly + 5 quarterly = 23 branded reports.

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

**Mboya Jeffers** — Data Engineer

- [GitHub](https://github.com/mboyajeffers)
- [LinkedIn](https://linkedin.com/in/mboyajeffers)
