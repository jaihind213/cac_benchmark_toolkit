"""
pipeline/clean.py
-----------------
Normalise schema and filter functionally invalid rows.
Reads schema mappings and standard column definitions from config/schema.yaml.

Steps:
  1. Detect schema variant from raw parquet columns
  2. Rename columns to standard names
  3. Cast columns to standard dtypes
  4. Standardise values (payment_type, RatecodeID string → int)
  5. Filter invalid rows → clean / dirty

Reads from:  data/extracted/entities/<entity>/
Writes to:   data/clean/entities/<entity>/
             data/dirty/entities/<entity>/

Usage:
    python -m pipeline.clean --entity trips
"""

import argparse
import csv
from datetime import datetime
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import yaml

from pipeline.timer import TimerLog, parse_years, entity_dir, paths_for_entity


def load_schema(schema_yaml: str) -> dict:
    with open(schema_yaml) as f:
        return yaml.safe_load(f)


# ── Schema normalisation ──────────────────────────────────────────────────────

def detect_and_rename(df: pd.DataFrame, schema: dict) -> pd.DataFrame:
    """Detect schema variant and rename columns to standard names."""
    mappings = schema.get("schema_mappings", [])
    for mapping in mappings:
        detect_col = mapping["detect_col"]
        if detect_col in df.columns:
            rename_map = mapping.get("rename", {})
            if rename_map:
                df = df.rename(columns=rename_map)
            return df
    return df  # already standard or unknown


def cast_and_standardise(df: pd.DataFrame, schema: dict) -> pd.DataFrame:
    """Cast columns to standard dtypes and standardise values."""
    std_cols = {c["name"]: c for c in schema.get("standard_columns", [])}

    for col_name, col_def in std_cols.items():
        if col_name not in df.columns:
            continue

        dtype    = col_def.get("dtype", "STRING")
        value_map = col_def.get("value_map", {})

        # apply value mapping first (before cast)
        if value_map:
            df[col_name] = df[col_name].astype(str).str.strip().str.lower().map(
                lambda x, vm=value_map: vm.get(x, x)
            )

        # cast to standard dtype
        try:
            if dtype == "TIMESTAMP":
                df[col_name] = pd.to_datetime(df[col_name], errors="coerce")
            elif dtype == "INT64":
                df[col_name] = pd.to_numeric(df[col_name], errors="coerce").astype("Int64")
            elif dtype == "FLOAT64":
                df[col_name] = pd.to_numeric(df[col_name], errors="coerce").astype("float64")
            elif dtype == "STRING":
                df[col_name] = df[col_name].astype(str).where(df[col_name].notna(), None)
        except Exception:
            pass  # leave as-is if cast fails

    return df


# ── Functional validation ─────────────────────────────────────────────────────

def split_clean_dirty(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    original = len(df)
    reasons  = pd.Series([""] * len(df), index=df.index)
    dirty    = pd.Series(False, index=df.index)

    # 1. pickup_datetime not null
    if "pickup_datetime" in df.columns:
        m = df["pickup_datetime"].isnull()
        reasons[m] += "null_pickup_datetime|"
        dirty |= m

    # 2. dropoff_datetime not null
    if "dropoff_datetime" in df.columns:
        m = df["dropoff_datetime"].isnull()
        reasons[m] += "null_dropoff_datetime|"
        dirty |= m

    # 3. pickup <= dropoff
    if "pickup_datetime" in df.columns and "dropoff_datetime" in df.columns:
        m = df["pickup_datetime"] > df["dropoff_datetime"]
        reasons[m] += "dropoff_before_pickup|"
        dirty |= m

    # 4. passenger_count: 0 <= x <= PAX_MAX
    PAX_MIN, PAX_MAX = 0, 20
    if "passenger_count" in df.columns:
        m = df["passenger_count"].notna() & ~df["passenger_count"].between(PAX_MIN, PAX_MAX)
        reasons[m] += "invalid_passenger_count|"
        dirty |= m

    # 5-13. money/distance: null or >= 0
    for col, label in [
        ("trip_distance",        "distance_negative"),
        ("fare_amount",          "fare_negative"),
        ("total_amount",         "total_negative"),
        ("tip_amount",           "tip_negative"),
        ("tolls_amount",         "tolls_negative"),
        ("mta_tax",              "mta_tax_negative"),
        ("extra",                "extra_negative"),
        ("congestion_surcharge", "congestion_surcharge_negative"),
        ("airport_fee",          "airport_fee_negative"),
    ]:
        if col in df.columns:
            m = pd.to_numeric(df[col], errors="coerce") < 0
            reasons[m] += f"{label}|"
            dirty |= m

    # 13. PULocationID not null
    if "PULocationID" in df.columns:
        m = df["PULocationID"].isnull()
        reasons[m] += "null_PULocationID|"
        dirty |= m

    # 14. DOLocationID not null
    if "DOLocationID" in df.columns:
        m = df["DOLocationID"].isnull()
        reasons[m] += "null_DOLocationID|"
        dirty |= m

    # 15. cab_type not null or empty
    if "cab_type" in df.columns:
        m = df["cab_type"].isnull() | (df["cab_type"].astype(str).str.strip() == "")
        reasons[m] += "null_cab_type|"
        dirty |= m

    # 16. VendorID not null or empty
    if "VendorID" in df.columns:
        m = df["VendorID"].isnull() | (df["VendorID"].astype(str).str.strip() == "")
        reasons[m] += "null_VendorID|"
        dirty |= m

    dirty_mask = dirty.values
    dirty_df   = df[dirty_mask].copy()
    clean_df   = df[~dirty_mask].copy()

    if len(dirty_df) > 0:
        dirty_df["dirty_reason"] = reasons[dirty_mask].values
    else:
        dirty_df["dirty_reason"] = pd.Series(dtype=str)

    reason_counts = {}
    if dirty_mask.any():
        reason_counts = (
            reasons[dirty_mask]
            .str.split("|")
            .explode()
            .loc[lambda s: s != ""]
            .value_counts()
            .to_dict()
        )

    stats = {
        "rows_in":       original,
        "rows_clean":    len(clean_df),
        "rows_dirty":    len(dirty_df),
        "dirty_pct":     round(len(dirty_df) / original * 100, 2) if original else 0,
        "reason_counts": reason_counts,
    }
    return clean_df, dirty_df, stats


# ── Main ──────────────────────────────────────────────────────────────────────
ONE_MILLION = 1024*1024
def clean(entity: str, schema_yaml: str = "config/schema.yaml",
          data_dir: str = "./data", years: list[int] = None, row_group_size= ONE_MILLION):
    schema     = load_schema(schema_yaml)
    files      = paths_for_entity(data_dir + "/enriched", entity, years)
    clean_root = entity_dir(data_dir + "/clean",  entity)
    dirty_root = entity_dir(data_dir + "/dirty",  entity)

    reports_dir = Path("reports")
    reports_dir.mkdir(exist_ok=True)
    report_file = reports_dir / f"clean_report_{entity}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    report_rows = []

    print(f"Cleaning entity={entity}  ({len(files)} files)")
    print(f"  clean → {clean_root}")
    print(f"  dirty → {dirty_root}")

    total_in = total_clean = total_dirty = 0

    for f in files:
        rel        = f.relative_to(entity_dir(data_dir + "/enriched", entity))
        out_path   = clean_root / rel
        dirty_path = dirty_root / rel

        out_path.parent.mkdir(parents=True, exist_ok=True)
        dirty_path.parent.mkdir(parents=True, exist_ok=True)

        with TimerLog("clean", scope=f.name) as t:
            df = pd.read_parquet(f)
            df = detect_and_rename(df, schema)
            df = cast_and_standardise(df, schema)
            clean_df, dirty_df, stats = split_clean_dirty(df)
            pq.write_table(pa.Table.from_pandas(clean_df), out_path,   compression="zstd", row_group_size=row_group_size)
            pq.write_table(pa.Table.from_pandas(dirty_df), dirty_path, compression="zstd")
            t.rows = stats["rows_clean"]

        total_in    += stats["rows_in"]
        total_clean += stats["rows_clean"]
        total_dirty += stats["rows_dirty"]

        report_rows.append({
            "entity":     entity,
            "file":       f.name,
            "rows_in":    stats["rows_in"],
            "rows_clean": stats["rows_clean"],
            "rows_dirty": stats["rows_dirty"],
            "dirty_pct":  stats["dirty_pct"],
            "reasons":    "|".join(f"{k}:{v}" for k, v in stats["reason_counts"].items()),
        })

        print(f"  {f.name}  in={stats['rows_in']:,}  "
              f"clean={stats['rows_clean']:,}  "
              f"dirty={stats['rows_dirty']:,} ({stats['dirty_pct']}%)")

        if stats["reason_counts"]:
            for reason, count in sorted(stats["reason_counts"].items()):
                print(f"      {reason}: {count:,}")

    if total_in:
        print(f"\n{'='*60}")
        print(f"TOTAL REPORT — entity={entity}")
        print(f"{'='*60}")
        processed_years = sorted(set(f.parent.name for f in files))
        print(f"  Years:      {', '.join(str(y) for y in processed_years)}")
        print(f"  Files:      {len(files):>12,}")
        print(f"  Rows in:    {total_in:>12,}")
        print(f"  Rows clean: {total_clean:>12,}  ({round(total_clean/total_in*100,2)}%)")
        print(f"  Rows dirty: {total_dirty:>12,}  ({round(total_dirty/total_in*100,2)}%)")
        print(f"{'='*60}")

        with open(report_file, "w", newline="") as rf:
            w = csv.DictWriter(rf, fieldnames=[
                "entity", "file", "rows_in", "rows_clean",
                "rows_dirty", "dirty_pct", "reasons"
            ])
            w.writeheader()
            w.writerows(report_rows)
        print(f"\nReport → {report_file}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--entity",   required=True)
    parser.add_argument("--schema",   default="config/schema.yaml")
    parser.add_argument("--years",    type=str, nargs="+", default=None)
    parser.add_argument("--data-dir", dest="data_dir", default="./data")
    parser.add_argument("--row-group-size", dest="row_group_size", type=int, default=ONE_MILLION,
                        help="Parquet row group size (default: 1000000)")
    args = parser.parse_args()
    years = parse_years(args.years) if args.years else None
    clean(entity=args.entity, schema_yaml=args.schema,
          data_dir=args.data_dir, years=years, row_group_size=args.row_group_size)


if __name__ == "__main__":
    main()
