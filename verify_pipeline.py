"""
verify_pipeline.py
------------------
Checks each pipeline stage for unexpected <NA> values in key columns.
Streams file by file to avoid OOM.

Usage:
    python3.11 verify_pipeline.py --entity trips
    python3.11 verify_pipeline.py --entity trips --years 2009 2010
"""

import argparse
import glob
from pathlib import Path

import pandas as pd


KEY_COLS = {
    "extracted":    ["cab_type", "year", "month", "trip_id"],
    "enriched":     ["cab_type", "year", "month", "trip_id", "entity_id"],
    "clean":        ["cab_type", "pickup_datetime", "dropoff_datetime"],
    "convolutions": ["pickup_date", "cab_type"],  # varies per convolution
}

CONV_KEY_COLS = {
    "conv_cab_type":          ["pickup_date", "cab_type"],
    "conv_passenger_count":   ["pickup_date", "passenger_count"],
    "conv_trip_distance":     ["pickup_date", "trip_distance"],
    "conv_pickup_hour":       ["pickup_date", "pickup_hour"],
    "conv_pickup_zone":       ["pickup_date", "PULocationID"],
    "conv_dropoff_zone":      ["pickup_date", "DOLocationID"],
    "conv_payment_type":      ["pickup_date", "payment_type"],
    "conv_rate_code":         ["pickup_date", "RatecodeID"],
    "conv_pax_year":          ["pickup_date", "passenger_count", "pickup_year"],
    "conv_pax_year_distance": ["pickup_date", "passenger_count", "pickup_year", "trip_distance"],
}


def check_stage(stage: str, entity: str, data_dir: str, years: list[int] = None):
    root  = Path(data_dir) / stage / "entities" / entity
    if not root.exists():
        print(f"  ✗ {stage}: not found at {root}")
        return

    if stage == "convolutions":
        check_convolutions(root)
        return

    pattern = str(root / "**" / "*.parquet")
    files   = sorted(glob.glob(pattern, recursive=True))
    if years:
        files = [f for f in files if any(f"/{y}/" in f for y in years)]

    if not files:
        print(f"  ✗ {stage}: no parquet files found")
        return

    key_cols    = KEY_COLS.get(stage, [])
    total_rows  = 0
    total_na    = {col: 0 for col in key_cols}
    files_with_na = []

    for f in files:
        import pyarrow.parquet as pq
        available = pq.read_schema(f).names
        df = pd.read_parquet(f, columns=[c for c in key_cols if c in available])
        total_rows += len(df)
        has_na = False
        for col in key_cols:
            if col in df.columns:
                n = df[col].isna().sum()
                total_na[col] += n
                if n > 0:
                    has_na = True
        if has_na:
            files_with_na.append(Path(f).name)

    print(f"\n  {stage.upper()} ({len(files)} files, {total_rows:,} rows)")
    all_ok = True
    for col, n in total_na.items():
        status = "✓" if n == 0 else f"✗ {n:,} NAs"
        print(f"    {col:<25} {status}")
        if n > 0:
            all_ok = False
    if not all_ok:
        print(f"    Files with NA: {files_with_na[:5]}" +
              (f" ... +{len(files_with_na)-5} more" if len(files_with_na) > 5 else ""))
    else:
        print(f"    All key columns clean ✓")


def check_convolutions(conv_root: Path):
    print(f"\n  CONVOLUTIONS")

    for conv_name, key_cols in CONV_KEY_COLS.items():
        files = sorted(conv_root.rglob(f"{conv_name}.parquet"))
        if not files:
            print(f"    {conv_name:<35} not found")
            continue

        total_rows  = 0
        total_na    = {col: 0 for col in key_cols}

        for f in files:
            df = pd.read_parquet(f)
            total_rows += len(df)
            for col in key_cols:
                if col in df.columns:
                    total_na[col] += int(df[col].isna().sum())

        na_summary = {col: n for col, n in total_na.items() if n > 0}
        if na_summary:
            print(f"    {conv_name:<35} ✗ NAs: {na_summary}")
        else:
            print(f"    {conv_name:<35} ✓  ({len(files)} files, {total_rows:,} rows)")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--entity",   default="trips")
    parser.add_argument("--data-dir", dest="data_dir", default="./data")
    parser.add_argument("--years",    type=int, nargs="+", default=None)
    args = parser.parse_args()

    print(f"Verifying pipeline for entity={args.entity}")
    print(f"{'='*60}")

    for stage in ["extracted", "enriched", "clean", "convolutions"]:
        check_stage(stage, args.entity, args.data_dir, args.years)

    print(f"\n{'='*60}")
    print("Done.")


if __name__ == "__main__":
    main()