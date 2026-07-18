"""
verify_facts.py
---------------
Sanity check on the facts parquet files.
Prints row counts, column stats, and flags invalid values.

Usage:
    python3.11 verify_facts.py
    python3.11 verify_facts.py --entity trips --data-dir ./data
"""

import argparse
import glob
from pathlib import Path

import pandas as pd


def verify_facts(entity: str, data_dir: str):
    facts_root = Path(data_dir) / "facts" / "entities" / entity
    if not facts_root.exists():
        print(f"Facts not found: {facts_root}")
        return

    files = sorted(facts_root.rglob("*.parquet"))
    print(f"Facts: {facts_root}")
    print(f"Files: {len(files)}")
    if not files:
        return

    # sample first file
    sample = pd.read_parquet(files[0])
    print(f"\nSample file: {files[0].name}")
    print(f"Columns: {list(sample.columns)}")
    print(f"Rows:    {len(sample):,}")

    # stream file by file — accumulate stats without loading all into memory
    print(f"\nStreaming {len(files)} files...")

    total_rows    = 0
    null_counts   = {}
    neg_counts    = {}
    invalid_pax   = 0
    dupe_ids      = set()
    all_entity_ids = set()
    min_vals      = {}
    max_vals      = {}
    sum_vals      = {}
    count_vals    = {}
    min_dt        = None
    max_dt        = None

    for i, f in enumerate(files):
        df = pd.read_parquet(f)
        total_rows += len(df)

        for col in df.columns:
            series = df[col]

            # null counts
            null_counts[col] = null_counts.get(col, 0) + series.isna().sum()

            if pd.api.types.is_numeric_dtype(series):
                s = series.dropna()
                if len(s):
                    neg_counts[col]  = neg_counts.get(col, 0) + int((s < 0).sum())
                    min_vals[col]    = min(min_vals.get(col, float('inf')), float(s.min()))
                    max_vals[col]    = max(max_vals.get(col, float('-inf')), float(s.max()))
                    sum_vals[col]    = sum_vals.get(col, 0.0) + float(s.sum())
                    count_vals[col]  = count_vals.get(col, 0) + len(s)

        # passenger_count validity
        if "passenger_count" in df.columns:
            bad = df["passenger_count"].notna() & ~df["passenger_count"].between(0, 6)
            invalid_pax += int(bad.sum())

        # pickup_datetime range
        if "pickup_datetime" in df.columns:
            dt = pd.to_datetime(df["pickup_datetime"], errors="coerce").dropna()
            if len(dt):
                min_dt = min(min_dt, dt.min()) if min_dt else dt.min()
                max_dt = max(max_dt, dt.max()) if max_dt else dt.max()

        if (i + 1) % 100 == 0:
            print(f"  {i+1}/{len(files)} files  {total_rows:,} rows so far...")

    print(f"\nTotal rows: {total_rows:,}")
    print(f"\n{'='*60}")
    print("COLUMN STATS")
    print(f"{'='*60}")

    for col in null_counts:
        null_pct = round(null_counts[col] / total_rows * 100, 2)
        print(f"\n{col}")
        print(f"  nulls: {null_counts[col]:,} ({null_pct}%)")
        if col in min_vals:
            mean = sum_vals[col] / count_vals[col] if count_vals[col] else 0
            print(f"  min:   {min_vals[col]}")
            print(f"  max:   {max_vals[col]}")
            print(f"  mean:  {mean:.4f}")
            print(f"  neg:   {neg_counts.get(col, 0):,}")

    print(f"\n{'='*60}")
    print("VALIDITY CHECKS")
    print(f"{'='*60}")
    print(f"passenger_count invalid (not 0-6): {invalid_pax:,}  ← should be 0")
    for col in ["fare_amount", "total_amount", "tip_amount", "tolls_amount", "trip_distance"]:
        print(f"{col} < 0: {neg_counts.get(col, 0):,}  ← should be 0")
    if min_dt:
        print(f"pickup_datetime range: {min_dt} → {max_dt}")
    print(f"{'='*60}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--entity",   default="trips")
    parser.add_argument("--data-dir", dest="data_dir", default="./data")
    args = parser.parse_args()
    verify_facts(args.entity, args.data_dir)


if __name__ == "__main__":
    main()
