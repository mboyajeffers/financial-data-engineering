#!/usr/bin/env python3
"""
Example: Validate SEC EDGAR company filings data.

Pulls real data from the SEC EDGAR full-text search API,
then validates it for completeness, uniqueness, and value ranges.

Data source: https://efts.sec.gov/LATEST/search-index?q=*
No API key required. SEC asks for a User-Agent header with contact info.

Usage:
    python examples/validate_sec_filings.py
"""

import sys
from pathlib import Path

import pandas as pd
import requests

# Allow imports from src/
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from quality import (
    DataValidator,
    CompletenessRule,
    UniquenessRule,
    RangeRule,
    PatternRule,
    CustomRule,
)

HEADERS = {
    'User-Agent': 'DataQualityFramework/1.0 (github.com/mboyajeffers)',
    'Accept': 'application/json',
}

COMPANY_TICKERS_URL = 'https://www.sec.gov/files/company_tickers.json'


def fetch_sec_company_tickers() -> pd.DataFrame:
    """
    Fetch the SEC company tickers file.

    Returns a DataFrame with columns: cik, name, ticker, exchange.
    This file contains ~10,000 public company registrations.
    """
    print("Fetching SEC company tickers...")
    resp = requests.get(COMPANY_TICKERS_URL, headers=HEADERS, timeout=30)
    resp.raise_for_status()

    data = resp.json()

    # SEC returns {0: {cik_str, ticker, title}, 1: {...}, ...}
    records = list(data.values())
    df = pd.DataFrame(records)

    # Normalize column names
    df = df.rename(columns={
        'cik_str': 'cik',
        'title': 'company_name',
        'ticker': 'ticker',
    })

    # Pad CIK to 10 digits (SEC standard format)
    df['cik'] = df['cik'].astype(str).str.zfill(10)

    print(f"  Fetched {len(df):,} companies from SEC EDGAR")
    return df


def fetch_sec_filings(cik: str, filing_type: str = '10-K', count: int = 40) -> pd.DataFrame:
    """
    Fetch recent filings for a single company.

    Args:
        cik: Central Index Key (10-digit padded).
        filing_type: Filing type to retrieve (10-K, 10-Q, 8-K, etc.).
        count: Maximum filings to return.
    """
    url = f'https://efts.sec.gov/LATEST/search-index?q=%22{cik}%22&dateRange=custom&startdt=2020-01-01&enddt=2025-12-31&forms={filing_type}'
    resp = requests.get(url, headers=HEADERS, timeout=30)

    if resp.status_code != 200:
        return pd.DataFrame()

    data = resp.json()
    hits = data.get('hits', {}).get('hits', [])

    if not hits:
        return pd.DataFrame()

    records = []
    for hit in hits[:count]:
        source = hit.get('_source', {})
        records.append({
            'cik': cik,
            'file_date': source.get('file_date'),
            'form_type': source.get('form_type'),
            'display_names': ', '.join(source.get('display_names', [])),
        })

    return pd.DataFrame(records)


def main():
    # --- Step 1: Fetch and validate company tickers ---
    tickers_df = fetch_sec_company_tickers()

    print("\n--- Validating SEC Company Tickers ---")

    v = DataValidator("sec_company_tickers")
    v.add_rule(CompletenessRule(
        columns=['cik', 'company_name', 'ticker'],
        name='required_fields_present',
    ))
    v.add_rule(UniquenessRule(
        columns=['cik'],
        name='cik_is_unique_key',
    ))
    v.add_rule(PatternRule(
        column='cik',
        pattern=r'^\d{10}$',
        name='cik_is_10_digit_padded',
    ))
    v.add_rule(PatternRule(
        column='ticker',
        pattern=r'^[A-Z]{1,5}$',
        name='ticker_is_valid_format',
    ))
    v.add_rule(CustomRule(
        func=lambda df: (
            len(df) >= 5000,
            {'row_count': len(df), 'minimum_expected': 5000},
        ),
        name='minimum_row_count',
    ))

    report = v.validate(tickers_df)
    report.print_summary()
    report.print_failures()

    # --- Step 2: Fetch and validate filings for a known company ---
    print("\n--- Validating SEC Filings (Apple Inc, CIK 0000320193) ---")

    filings_df = fetch_sec_filings('0000320193', filing_type='10-K')

    if filings_df.empty:
        print("  No filings returned (API may be rate-limited). Skipping.")
    else:
        print(f"  Fetched {len(filings_df)} filings")

        fv = DataValidator("apple_10k_filings")
        fv.add_rule(CompletenessRule(
            columns=['cik', 'file_date', 'form_type'],
            name='filing_fields_complete',
        ))
        fv.add_rule(PatternRule(
            column='file_date',
            pattern=r'^\d{4}-\d{2}-\d{2}$',
            name='file_date_is_iso_format',
        ))

        filing_report = fv.validate(filings_df)
        filing_report.print_summary()
        filing_report.print_failures()

    # --- Summary ---
    print("\nSample data (first 10 rows):")
    print(tickers_df[['cik', 'company_name', 'ticker']].head(10).to_string(index=False))
    print()


if __name__ == '__main__':
    main()
