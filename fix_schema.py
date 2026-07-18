"""
fix_schema.py
-------------
One-off script to normalise column names and standardise values
in already-enriched parquet files, using config/schema.yaml.

Avoids re-running the full 4-hour enrich pipeline.

Reads from:  data/enriched/entities/<entity>/
Writes to:   data/enriched_fix/entities/<entity>/

Usage:
    python3.11 fix_schema.py --entity trips
    python3.11 fix_schema.py --in ./data/enriched --out ./data/enriched_fix --entity trips
"""

import argparse
import time
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import yaml


def load_schema(schema_yaml: str) -> dict:
    with open(schema_yaml) as f:
        return yaml.safe_load(f)


def detect_and_rename(df: pd.DataFrame, schema: dict) -> pd.DataFrame:
    for mapping in schema.get("schema_mappings", []):
        if mapping["detect_col"] in df.columns:
            rename_map = mapping.get("rename", {})
            if rename_map:
                df = df.rename(columns=rename_map)
            return df
    return df


def cast_and_standardise(df: pd.DataFrame, schema: dict) -> pd.DataFrame:
    std_cols = {c["name"]: c for c in schema.get("standard_columns", [])}
    for col_name, col_def in std_cols.items():
        if col_name not in df.columns:
            continue
        dtype     = col_def.get("dtype", "STRING")
        value_map = col_def.get("value_map", {})
        if value_map:
            df[col_name] = df[col_name].astype(str).str.strip().str.lower().map(
                lambda x, vm=value_map: vm.get(x, x)
            )
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
            pass
    return df


def fix(in_dir: str, out_dir: str, entity: str,
        schema_yaml: str = "config/schema.yaml", force: bool = False):
    schema   = load_schema(schema_yaml)
    in_root  = Path(in_dir)  / "entities" / entity
    out_root = Path(out_dir) / "entities" / entity
    files    = sorted(in_root.rglob("*.parquet"))

    print(f"Fixing {len(files)} files using {schema_yaml}")
    print(f"  from: {in_root}")
    print(f"  to:   {out_root}")

    total = 0
    for f in files:
        rel      = f.relative_to(in_root)
        out_path = out_root / rel
        if out_path.exists() and not force:
            print(f"  skipping {f.name} — already exists")
            continue
        t0 = time.perf_counter()
        df = pd.read_parquet(f)
        df = detect_and_rename(df, schema)
        df = cast_and_standardise(df, schema)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(pa.Table.from_pandas(df), out_path, compression="zstd")
        elapsed = (time.perf_counter() - t0) * 1000
        total  += len(df)
        print(f"  ✓ {f.name}  {len(df):,} rows  {elapsed:.0f}ms")

    print(f"\nDone — {total:,} rows → {out_root}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--in",     dest="in_dir",  default="./data/enriched")
    parser.add_argument("--out",    dest="out_dir",  default="./data/enriched_fix")
    parser.add_argument("--entity", default="trips")
    parser.add_argument("--schema", default="config/schema.yaml")
    parser.add_argument("--force",  action="store_true")
    args = parser.parse_args()
    fix(args.in_dir, args.out_dir, args.entity, args.schema, args.force)


if __name__ == "__main__":
    main()
