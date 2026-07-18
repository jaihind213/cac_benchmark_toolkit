"""
pipeline/download.py
--------------------
Downloads NYC TLC taxi Parquet files to data/raw/entities/<entity>/

Usage:
    python -m pipeline.download --entity trips --years 2009-2015
    python -m pipeline.download --entity trips --years 2009 2010 --taxi-types yellow green
"""

import argparse
import time
from pathlib import Path

import requests

from pipeline.timer import TimerLog, parse_years, entity_dir

BASE_URL  = "https://d37ci6vzurychx.cloudfront.net/trip-data"
ZONE_URL  = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"

TAXI_PREFIXES = {
    "yellow": "yellow_tripdata",
    "green":  "green_tripdata",
    "fhv":    "fhv_tripdata",
}

# green started Aug 2013, HVFHV Feb 2019
GREEN_START = (2013, 8)
HVFHV_START = (2019, 2)


def should_download(taxi_type: str, year: int, month: int) -> bool:
    if taxi_type == "green" and (year, month) < GREEN_START:
        return False
    if taxi_type == "fhv" and (year, month) < HVFHV_START:
        return False
    return True


def download_file(url: str, out_path: Path, max_retries: int = 5) -> bool:
    for attempt in range(max_retries):
        try:
            t0 = time.time()
            r  = requests.get(url, stream=True, timeout=120)
            if r.status_code == 403:
                print(f"  skipping {out_path.name} — not available (403)")
                return False
            r.raise_for_status()
            out_path.parent.mkdir(parents=True, exist_ok=True)
            with open(out_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=65536):
                    f.write(chunk)
            sz = out_path.stat().st_size
            print(f"  ✓ {out_path.name}  {sz/1e6:.1f} MB  ({time.time()-t0:.1f}s)")
            return True
        except Exception as e:
            wait = 2 ** attempt
            print(f"  retry {attempt+1}/{max_retries}: {e}  (wait {wait}s)")
            time.sleep(wait)
    print(f"  ✗ FAILED: {url}")
    return False


def download(entity: str, years: list[int], months: list[int],
             taxi_types: list[str], raw_dir: str = "./data"):
    out = entity_dir(raw_dir + "/raw", entity)
    out.mkdir(parents=True, exist_ok=True)

    # zone lookup alongside entity data
    zone_path = out / "taxi_zone_lookup.csv"
    if not zone_path.exists():
        print("Downloading zone lookup ...")
        with TimerLog("download", scope="zone_lookup"):
            download_file(ZONE_URL, zone_path)
    else:
        print(f"Zone lookup already exists: {zone_path}")

    for taxi_type in taxi_types:
        prefix = TAXI_PREFIXES[taxi_type]
        for year in sorted(years):
            for month in sorted(months):
                if not should_download(taxi_type, year, month):
                    continue
                fname    = f"{prefix}_{year}-{month:02d}.parquet"
                url      = f"{BASE_URL}/{fname}"
                out_path = out / str(year) / fname
                if out_path.exists():
                    print(f"  already exists: {fname}")
                    continue
                print(f"  {fname} ...")
                with TimerLog("download", scope=f"{taxi_type}-{year}-{month:02d}"):
                    download_file(url, out_path)

    print(f"\nDownload complete → {out}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--entity",     required=True, help="Entity name e.g. trips")
    parser.add_argument("--years",      nargs="+", required=True,
                        help="Years e.g. 2009 2010 OR 2009-2015")
    parser.add_argument("--months",     type=int, nargs="+", default=list(range(1, 13)))
    parser.add_argument("--taxi-types", nargs="+", default=["yellow", "green"],
                        choices=["yellow", "green", "fhv"])
    parser.add_argument("--data-dir",   dest="data_dir", default="./data")
    args = parser.parse_args()

    years = parse_years(args.years)
    print(f"Entity: {args.entity}")
    print(f"Years:  {years}")
    print(f"Types:  {args.taxi_types}")

    download(
        entity=args.entity,
        years=years,
        months=args.months,
        taxi_types=args.taxi_types,
        raw_dir=args.data_dir,
    )


if __name__ == "__main__":
    main()
