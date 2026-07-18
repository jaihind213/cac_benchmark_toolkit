"""
pipeline/validate.py
--------------------
Validates downloaded NYC taxi Parquet files.
Checks row counts, schema, null rates, and obvious outliers.

Usage:
    python -m pipeline.validate --in ./data/raw --year 2024
"""

import argparse
from pathlib import Path

import pyarrow.parquet as pq
import pandas as pd

from pipeline.timer import TimerLog, parse_years


EXPECTED_COLS = {
    "VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime",
    "passenger_count", "trip_distance", "RatecodeID",
    "PULocationID", "DOLocationID", "payment_type",
    "fare_amount", "extra", "mta_tax", "tip_amount",
    "tolls_amount", "improvement_surcharge", "total_amount",
    "congestion_surcharge",
}


def validate_file(path: Path) -> dict:
    pf  = pq.read_table(path)
    df  = pf.to_pandas()
    cols = set(df.columns)

    missing_cols = EXPECTED_COLS - cols
    extra_cols   = cols - EXPECTED_COLS

    issues = []
    if missing_cols:
        issues.append(f"Missing columns: {missing_cols}")

    # null rates
    null_rates = df.isnull().mean()
    high_null  = null_rates[null_rates > 0.05].to_dict()
    if high_null:
        issues.append(f"High null rate columns (>5%): {high_null}")

    # obvious outliers (before cleaning)
    if "passenger_count" in df.columns:
        bad_pax = (df["passenger_count"] < 0).sum()
        if bad_pax:
            issues.append(f"{bad_pax} rows with negative passenger_count")

    if "trip_distance" in df.columns:
        bad_dist = (df["trip_distance"] < 0).sum()
        if bad_dist:
            issues.append(f"{bad_dist} rows with negative trip_distance")

    if "fare_amount" in df.columns:
        bad_fare = (df["fare_amount"] < 0).sum()
        if bad_fare:
            issues.append(f"{bad_fare} rows with negative fare_amount")

    return {
        "file":        path.name,
        "rows":        len(df),
        "cols":        len(cols),
        "extra_cols":  extra_cols,
        "issues":      issues,
        "ok":          len(issues) == 0,
    }


def validate(in_dir: str, years: list[int] = None):
    root = Path(in_dir)

    if years:
        files = sorted(f for y in years for f in root.glob(f"{y}/**/*.parquet"))
    else:
        files = sorted(root.glob("**/*.parquet"))

    if not files:
        print(f"No parquet files found in {in_dir}")
        return

    print(f"Validating {len(files)} file(s) ...")
    total_rows = 0
    all_ok = True

    for f in files:
        with TimerLog("validate", scope=f.name):
            result = validate_file(f)

        total_rows += result["rows"]
        status = "✓" if result["ok"] else "✗"
        print(f"  {status} {result['file']}  {result['rows']:,} rows  {result['cols']} cols")

        if result["issues"]:
            all_ok = False
            for issue in result["issues"]:
                print(f"      ⚠ {issue}")

    print(f"\nTotal rows: {total_rows:,}")
    print(f"Validation: {'PASSED ✓' if all_ok else 'ISSUES FOUND ✗'}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--in",    dest="in_dir", default="./data/raw")
    parser.add_argument("--years", type=str, nargs="+", default=None,
                        help="Year(s) to validate e.g. 2009 2010 OR omit for all")
    args = parser.parse_args()
    years = parse_years(args.years) if args.years else None
    validate(in_dir=args.in_dir, years=years)


if __name__ == "__main__":
    main()
