"""
Single-source demo: USGS Earthquake Data

Fetches M4.5+ earthquakes from the last 30 days,
prints extraction telemetry and geographic distribution.
"""

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

# Allow running from project root
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from collector import USGSClient


def main():
    client = USGSClient()

    end = datetime.now(timezone.utc)
    start = end - timedelta(days=30)

    print("=" * 60)
    print("USGS Earthquake Collection — Last 30 Days")
    print("=" * 60)
    print(f"Date range: {start:%Y-%m-%d} to {end:%Y-%m-%d}")
    print(f"Minimum magnitude: 4.5")
    print()

    result = client.extract(
        start_date=start.strftime("%Y-%m-%d"),
        end_date=end.strftime("%Y-%m-%d"),
        min_magnitude=4.5,
        max_results=2000,
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

    # Top 10 by magnitude
    print("--- Top 10 Earthquakes by Magnitude ---")
    top = df.nlargest(10, "magnitude")[["magnitude", "place", "time", "depth"]]
    for _, row in top.iterrows():
        print(f"  M{row['magnitude']:.1f}  {row['place']}")
    print()

    # Geographic distribution
    print("--- Geographic Distribution ---")
    if "place" in df.columns:
        # Extract rough region from place description
        regions = df["place"].str.extract(r",\s*(.+)$")[0].value_counts().head(10)
        for region, count in regions.items():
            print(f"  {region}: {count} events")
    print()

    # Depth statistics
    print("--- Depth Statistics ---")
    print(f"  Mean depth:   {df['depth'].mean():.1f} km")
    print(f"  Median depth: {df['depth'].median():.1f} km")
    print(f"  Max depth:    {df['depth'].max():.1f} km")


if __name__ == "__main__":
    main()
