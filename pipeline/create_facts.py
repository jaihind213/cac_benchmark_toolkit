"""
pipeline/create_facts.py
------------------------
Creates slim fact tables from enriched parquet files.
Extracts entity_id + fact_cols defined in entities.yaml.

Reads from:  data/clean/entities/<entity>/  (or --input subdir)
Writes to:   data/facts/entities/<entity>/

This is a standalone step — can be run independently of enrich.py
or re-run to update fact_cols without re-running full enrich pipeline.

Usage:
    python -m pipeline.create_facts --entity trips
    python -m pipeline.create_facts --entity trips --years 2009-2015
    python -m pipeline.create_facts --entity trips --input clean --row-group-size 500000
"""

import argparse
import time
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import yaml

from pipeline.timer import TimerLog, parse_years, entity_dir, paths_for_entity


def load_entity_config(entities_yaml: str, entity_name: str) -> dict:
    with open(entities_yaml) as f:
        configs = yaml.safe_load(f)["entities"]
    for cfg in configs:
        if cfg["name"] == entity_name:
            return cfg
    raise ValueError(f"Entity '{entity_name}' not found in {entities_yaml}")


def write_fact_file(df: pd.DataFrame, fact_cols: list[dict],
                    primary_ts: dict, out_root: Path, filename: str,
                    row_group_size: int = 1_000_000) -> int:
    """Partition by primary_timestamp; write only entity_id + fact_cols to parquet."""
    fact_df = df.copy()

    # cast fact_col dtypes
    for fc in fact_cols:
        col   = fc["col"]
        dtype = fc.get("dtype")
        if col not in fact_df.columns or not dtype:
            continue
        try:
            if dtype == "TIMESTAMP":
                fact_df[col] = pd.to_datetime(fact_df[col], errors="coerce")
            elif dtype in ("float32", "float64"):
                fact_df[col] = pd.to_numeric(fact_df[col], errors="coerce").astype(dtype)
            elif dtype in ("int8", "Int8", "int64", "Int64"):
                fact_df[col] = pd.to_numeric(fact_df[col], errors="coerce").astype(dtype)
        except Exception:
            pass

    # columns to KEEP in the final parquet: entity_id + declared fact_cols
    keep_cols = ["entity_id"] + [fc["col"] for fc in fact_cols
                                 if fc["col"] in fact_df.columns]

    # derive partition path from primary_timestamp
    ts_col     = primary_ts.get("col", "pickup_datetime")
    partitions = primary_ts.get("partitions", ["year", "month", "day"])

    total = 0
    if ts_col in fact_df.columns:
        dt = pd.to_datetime(fact_df[ts_col], errors="coerce")

        part_series = ""
        if "year"  in partitions: part_series = dt.dt.year.astype(str).str.zfill(4)
        if "month" in partitions:
            m = dt.dt.month.astype(str).str.zfill(2)
            part_series = part_series + "/" + m if len(part_series) else m
        if "day"   in partitions:
            d = dt.dt.day.astype(str).str.zfill(2)
            part_series = part_series + "/" + d if len(part_series) else d

        fact_df["_part"] = part_series.values

        for part_val, grp in fact_df.groupby("_part", sort=True):
            out_dir = out_root / part_val
            out_dir.mkdir(parents=True, exist_ok=True)
            out_file = out_dir / filename
            # final prune: only entity_id + fact_cols hit disk
            grp = grp[keep_cols].sort_values("entity_id").reset_index(drop=True)
            pq.write_table(
                pa.Table.from_pandas(grp),
                out_file,
                compression="zstd",
                row_group_size=row_group_size
            )
            total += len(grp)
    return total


def create_facts(entity: str, entities_yaml: str, data_dir: str = "./data",
                 enriched_subdir: str = "clean", years: list[int] = None,
                 row_group_size: int = 1_000_000):

    entity_cfg  = load_entity_config(entities_yaml, entity)
    fact_cols   = entity_cfg.get("fact_cols", [])
    primary_ts  = entity_cfg.get("primary_timestamp", {})

    if not fact_cols:
        print(f"No fact_cols defined for entity '{entity}' in {entities_yaml}")
        return

    enriched_root = entity_dir(f"{data_dir}/{enriched_subdir}", entity)
    facts_root    = entity_dir(f"{data_dir}/facts", entity)

    if years:
        files = sorted(f for y in years for f in enriched_root.glob(f"{y}/**/*.parquet"))
    else:
        files = sorted(enriched_root.rglob("*.parquet"))

    fact_col_names = ["entity_id"] + [fc["col"] for fc in fact_cols]
    print(f"Creating facts for entity={entity}  ({len(files)} files)")
    print(f"  fact_cols: {fact_col_names}")
    print(f"  partition: {primary_ts.get('partitions', ['year','month','day'])}")
    print(f"  row_group_size: {row_group_size:,}")
    print(f"  from: {enriched_root}")
    print(f"  to:   {facts_root}")

    total_rows = 0
    total_ms   = 0.0

    for f in files:
        t0   = time.perf_counter()
        df   = pd.read_parquet(f)
        rows = write_fact_file(df, fact_cols, primary_ts, facts_root, f.name,
                               row_group_size=row_group_size)
        elapsed_ms = (time.perf_counter() - t0) * 1000

        total_rows += rows
        total_ms   += elapsed_ms
        print(f"  ✓ {f.name}  {rows:,} rows  {elapsed_ms:.0f}ms")

    if files:
        print(f"\n{'='*50}")
        print(f"  Total rows: {total_rows:,}")
        print(f"  Total time: {total_ms/1000:.1f}s")
        print(f"{'='*50}")
    print(f"\nFacts → {facts_root}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--entity",   required=True)
    parser.add_argument("--entities", default="config/entities.yaml")
    parser.add_argument("--years",    type=str, nargs="+", default=None)
    parser.add_argument("--data-dir", dest="data_dir", default="./data")
    parser.add_argument("--input",  dest="enriched_subdir", default="clean",
                        help="Subdir under data-dir to read from (default: clean)")
    parser.add_argument("--row-group-size", dest="row_group_size", type=int, default=1_000_000,
                        help="Parquet row group size (default: 1000000)")
    args = parser.parse_args()
    years = parse_years(args.years) if args.years else None

    create_facts(
        entity=args.entity,
        entities_yaml=args.entities,
        data_dir=args.data_dir,
        enriched_subdir=args.enriched_subdir,
        years=years,
        row_group_size=args.row_group_size,
    )


if __name__ == "__main__":
    main()