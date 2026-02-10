"""
Multi-source fusion demo (showcase).

Extracts from all three APIs, merges results on shared keys,
and prints a combined summary with per-source telemetry.

- USGS: Earthquake counts by region
- World Bank: GDP per capita + population
- Open-Meteo: Average temperature for capital cities
"""

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.extractors.usgs import USGSClient
from src.extractors.world_bank import WorldBankClient
from src.extractors.open_meteo import OpenMeteoClient
from src.pipeline.orchestrator import MultiSourceCollector


# Countries with their capital-city coordinates
COUNTRIES = {
    "US": {"name": "United States", "lat": 38.90, "lon": -77.04, "city": "Washington DC"},
    "JP": {"name": "Japan",         "lat": 35.68, "lon": 139.69, "city": "Tokyo"},
    "GB": {"name": "United Kingdom","lat": 51.51, "lon": -0.13,  "city": "London"},
    "DE": {"name": "Germany",       "lat": 52.52, "lon": 13.41,  "city": "Berlin"},
    "AU": {"name": "Australia",     "lat": -35.28,"lon": 149.13, "city": "Canberra"},
    "BR": {"name": "Brazil",        "lat": -15.79,"lon": -47.88, "city": "Brasilia"},
    "IN": {"name": "India",         "lat": 28.61, "lon": 77.21,  "city": "New Delhi"},
    "CA": {"name": "Canada",        "lat": 45.42, "lon": -75.70, "city": "Ottawa"},
}


def main():
    print("=" * 70)
    print("Multi-Source ETL Pipeline — Country Economic & Climate Profile")
    print("=" * 70)
    print()

    # Set up collector
    collector = MultiSourceCollector()
    collector.register("usgs", USGSClient())
    collector.register("world_bank", WorldBankClient())
    collector.register("open_meteo", OpenMeteoClient())

    end = datetime.now(timezone.utc)
    start = end - timedelta(days=365)

    country_codes = list(COUNTRIES.keys())
    locations = [
        (info["lat"], info["lon"], info["city"])
        for info in COUNTRIES.values()
    ]

    # Collect from all sources
    print("Collecting from 3 APIs...")
    print()

    results = collector.collect_all(
        usgs={
            "start_date": start.strftime("%Y-%m-%d"),
            "end_date": end.strftime("%Y-%m-%d"),
            "min_magnitude": 4.5,
            "max_results": 5000,
        },
        world_bank={
            "countries": country_codes,
            "indicators": ["NY.GDP.PCAP.CD", "SP.POP.TOTL"],
            "start_year": 2020,
            "end_year": 2023,
        },
        open_meteo={
            "locations": locations,
            "start_date": start.strftime("%Y-%m-%d"),
            "end_date": end.strftime("%Y-%m-%d"),
        },
    )

    # Per-source summary
    print("--- Per-Source Results ---")
    for name, result in results.items():
        status = "OK" if result.success else "FAILED"
        print(f"  {name:12s}  {status:6s}  {result.records:>6} records  "
              f"{result.api_calls} calls  {result.duration_seconds:.1f}s")
    print()

    # Aggregate telemetry
    telemetry = collector.get_telemetry()
    totals = telemetry["totals"]
    print("--- Aggregate Telemetry ---")
    print(f"  Total API calls:  {totals['api_calls']}")
    print(f"  Total cache hits: {totals['cache_hits']}")
    print(f"  Total errors:     {totals['errors']}")
    print()

    # Build combined table
    if not all(r.success for r in results.values()):
        failed = [n for n, r in results.items() if not r.success]
        print(f"Some sources failed: {failed}")
        print("Showing available data only.")
        print()

    # GDP per capita (latest year per country)
    wb_data = results.get("world_bank")
    gdp_map = {}
    pop_map = {}
    if wb_data and wb_data.success and wb_data.data is not None:
        wb_df = wb_data.data
        gdp = wb_df[wb_df["indicator_code"] == "NY.GDP.PCAP.CD"].dropna(subset=["value"])
        if not gdp.empty:
            latest = gdp.loc[gdp.groupby("country_code")["year"].idxmax()]
            for _, row in latest.iterrows():
                gdp_map[row["country_code"]] = row["value"]

        pop = wb_df[wb_df["indicator_code"] == "SP.POP.TOTL"].dropna(subset=["value"])
        if not pop.empty:
            latest = pop.loc[pop.groupby("country_code")["year"].idxmax()]
            for _, row in latest.iterrows():
                pop_map[row["country_code"]] = row["value"]

    # Average temperature per city
    temp_map = {}
    weather_data = results.get("open_meteo")
    if weather_data and weather_data.success and weather_data.data is not None:
        w_df = weather_data.data
        if "temperature_max" in w_df.columns:
            avg_temps = w_df.groupby("location")["temperature_max"].mean()
            temp_map = avg_temps.to_dict()

    # Print combined summary
    print("--- Combined Country Profile ---")
    print(f"{'Country':<20s} {'GDP/Cap':>12s} {'Population':>14s} {'Avg Temp':>10s}")
    print("-" * 60)
    for code, info in COUNTRIES.items():
        gdp_val = gdp_map.get(code)
        pop_val = pop_map.get(code)
        temp_val = temp_map.get(info["city"])

        gdp_str = f"${gdp_val:,.0f}" if gdp_val else "N/A"
        pop_str = f"{pop_val / 1e6:,.1f}M" if pop_val else "N/A"
        temp_str = f"{temp_val:.1f}C" if temp_val else "N/A"

        print(f"  {info['name']:<18s} {gdp_str:>12s} {pop_str:>14s} {temp_str:>10s}")

    print()
    print("Data sources: USGS, World Bank, Open-Meteo (all public, no API keys)")


if __name__ == "__main__":
    main()
