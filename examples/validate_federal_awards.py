#!/usr/bin/env python3
"""
Example: Validate federal contract awards from USASpending.gov.

Pulls real award data from the USASpending API, then validates
completeness, uniqueness, value ranges, and referential integrity.

Data source: https://api.usaspending.gov/
No API key required. Public government data.

Usage:
    python examples/validate_federal_awards.py
"""

import sys
from pathlib import Path

import pandas as pd
import requests

sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from quality import (
    DataValidator,
    CompletenessRule,
    UniquenessRule,
    RangeRule,
    CustomRule,
)

USASPENDING_URL = 'https://api.usaspending.gov/api/v2/search/spending_by_award/'


def fetch_federal_awards(limit: int = 500) -> pd.DataFrame:
    """
    Fetch recent federal contract awards from USASpending.gov.

    Returns a DataFrame with award ID, recipient, amount, agency, and date.
    """
    print("Fetching federal awards from USASpending.gov...")

    payload = {
        'filters': {
            'time_period': [
                {'start_date': '2024-01-01', 'end_date': '2025-12-31'}
            ],
            'award_type_codes': ['A', 'B', 'C', 'D'],  # Contracts only
        },
        'fields': [
            'Award ID',
            'Recipient Name',
            'Award Amount',
            'Awarding Agency',
            'Start Date',
            'End Date',
            'Award Type',
            'Description',
        ],
        'limit': limit,
        'page': 1,
        'sort': 'Award Amount',
        'order': 'desc',
    }

    resp = requests.post(USASPENDING_URL, json=payload, timeout=30)
    resp.raise_for_status()

    data = resp.json()
    results = data.get('results', [])

    if not results:
        return pd.DataFrame()

    df = pd.DataFrame(results)

    # Normalize column names to snake_case
    df = df.rename(columns={
        'Award ID': 'award_id',
        'Recipient Name': 'recipient_name',
        'Award Amount': 'award_amount',
        'Awarding Agency': 'awarding_agency',
        'Start Date': 'start_date',
        'End Date': 'end_date',
        'Award Type': 'award_type',
        'Description': 'description',
    })

    # Cast amount to float
    df['award_amount'] = pd.to_numeric(df['award_amount'], errors='coerce')

    print(f"  Fetched {len(df):,} federal contract awards")
    return df


def main():
    df = fetch_federal_awards(limit=500)

    if df.empty:
        print("No data returned from USASpending API. Exiting.")
        return

    print(f"\n--- Validating {len(df):,} Federal Awards ---")

    v = DataValidator("federal_contract_awards")

    # Required fields must be present
    v.add_rule(CompletenessRule(
        columns=['award_id', 'recipient_name', 'award_amount', 'awarding_agency'],
        name='required_fields_present',
    ))

    # Award IDs should be unique
    v.add_rule(UniquenessRule(
        columns=['award_id'],
        name='award_id_is_unique',
    ))

    # Award amounts should be positive (these are contract obligations)
    v.add_rule(RangeRule(
        column='award_amount',
        min_val=0,
        name='award_amount_non_negative',
    ))

    # No single contract should exceed $50B (sanity check)
    v.add_rule(RangeRule(
        column='award_amount',
        max_val=50_000_000_000,
        name='award_amount_under_50B',
    ))

    # At least 100 rows expected
    v.add_rule(CustomRule(
        func=lambda d: (
            len(d) >= 100,
            {'row_count': len(d), 'minimum_expected': 100},
        ),
        name='minimum_row_count',
    ))

    # Top agency should appear more than once (data diversity check)
    v.add_rule(CustomRule(
        func=lambda d: (
            d['awarding_agency'].nunique() >= 5,
            {
                'unique_agencies': int(d['awarding_agency'].nunique()),
                'minimum_expected': 5,
            },
        ),
        name='agency_diversity',
    ))

    report = v.validate(df)
    report.print_summary()
    report.print_failures()

    # Show sample data
    print("\nTop 10 awards by amount:")
    top = df.nlargest(10, 'award_amount')[
        ['award_id', 'recipient_name', 'award_amount', 'awarding_agency']
    ].copy()
    top['award_amount'] = top['award_amount'].apply(lambda x: f"${x:,.0f}")
    print(top.to_string(index=False))

    # Show data profile
    print("\nData Profile:")
    print(f"  Total awards:      {len(df):,}")
    print(f"  Unique recipients: {df['recipient_name'].nunique():,}")
    print(f"  Unique agencies:   {df['awarding_agency'].nunique():,}")
    print(f"  Total value:       ${df['award_amount'].sum():,.0f}")
    print(f"  Median award:      ${df['award_amount'].median():,.0f}")
    print()


if __name__ == '__main__':
    main()
