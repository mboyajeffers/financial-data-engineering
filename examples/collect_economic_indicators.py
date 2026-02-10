"""
Single-source demo: World Bank Economic Indicators

Fetches GDP per capita and population for major economies,
demonstrates page-number pagination across multiple indicators.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from src.extractors.world_bank import WorldBankClient


def main():
    client = WorldBankClient()

    countries = ["US", "GB", "JP", "DE", "FR", "CA", "AU", "BR", "IN", "CN"]

    print("=" * 60)
    print("World Bank Economic Indicators — G10 Economies")
    print("=" * 60)
    print(f"Countries:  {', '.join(countries)}")
    print("Indicators: GDP per capita, Population")
    print("Years:      2018-2023")
    print()

    result = client.extract(
        countries=countries,
        indicators=["NY.GDP.PCAP.CD", "SP.POP.TOTL"],
        start_year=2018,
        end_year=2023,
    )

    # Telemetry
    print("--- Telemetry ---")
    print(f"  Success:    {result.success}")
    print(f"  Records:    {result.records}")
    print(f"  API calls:  {result.api_calls}")
    print(f"  Cache hits: {result.cache_hits}")
    print(f"  Duration:   {result.duration_seconds:.2f}s")
    print()

    if not result.success:
        print(f"Error: {result.error}")
        return

    df = result.data

    # GDP per capita — latest year
    print("--- GDP per Capita (Latest Available Year) ---")
    gdp = df[df["indicator_code"] == "NY.GDP.PCAP.CD"].dropna(subset=["value"])
    if not gdp.empty:
        latest = gdp.loc[gdp.groupby("country_code")["year"].idxmax()]
        latest = latest.sort_values("value", ascending=False)
        for _, row in latest.iterrows():
            print(f"  {row['country_name']:20s}  ${row['value']:>12,.0f}  ({int(row['year'])})")
    print()

    # Population — latest year
    print("--- Population (Latest Available Year) ---")
    pop = df[df["indicator_code"] == "SP.POP.TOTL"].dropna(subset=["value"])
    if not pop.empty:
        latest = pop.loc[pop.groupby("country_code")["year"].idxmax()]
        latest = latest.sort_values("value", ascending=False)
        for _, row in latest.iterrows():
            pop_m = row["value"] / 1_000_000
            print(f"  {row['country_name']:20s}  {pop_m:>8,.1f}M  ({int(row['year'])})")
    print()

    # Data coverage
    print("--- Data Coverage ---")
    print(f"  Total records:    {len(df)}")
    print(f"  Non-null values:  {df['value'].notna().sum()}")
    print(f"  Null values:      {df['value'].isna().sum()}")
    print(f"  Countries:        {df['country_code'].nunique()}")
    print(f"  Year range:       {int(df['year'].min())}-{int(df['year'].max())}")


if __name__ == "__main__":
    main()
