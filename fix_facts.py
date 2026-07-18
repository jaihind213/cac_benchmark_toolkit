"""
fix_facts.py
------------
Rebuilds fact tables from already-enriched parquet files.
Avoids re-running the full 4-hour enrich pipeline.

Reads from:  data/enriched_fix/entities/<entity>/
Writes to:   data/facts_fix/entities/<entity>/

Usage:
    python3.11 fix_facts.py
    python3.11 fix_facts.py --in ./data/enriched_fix --out ./data/facts_fix --entity trips
"""

import argparse
import time
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import yaml


FACT_COLS = [
    ("entity_id",       None),
    ("pickup_datetime", "TIMESTAMP"),
    ("fare_amount",     "float32"),
    ("total_amount",    "float32"),
    ("tip_amount",      "float32"),
    ("tolls_amount",    "float32"),
    ("trip_distance",   "float32"),
    ("passenger_count", "Int8"),
]


def fix_facts(in_dir: str, out_dir: str, entity: str,
              entities_yaml: str = "config/entities.yaml",
              force: bool = False):

    # load fact_cols from entities.yaml
    with open(entities_yaml) as f:
        configs = yaml.safe_load(f)["entities"]
    entity_cfg = next((e for e in configs if e["name"] == entity), None)
    if not entity_cfg:
        raise ValueError(f"Entity '{entity}' not found in {entities_yaml}")

    fact_cols = [("entity_id", None)] + [
        (fc["col"], fc.get("dtype")) for fc in entity_cfg.get("fact_cols", [])
    ]

    in_root  = Path(in_dir)  / "entities" / entity
    out_root = Path(out_dir) / "entities" / entity
    files    = sorted(in_root.rglob("*.parquet"))

    print(f"Rebuilding facts for entity={entity}  ({len(files)} files)")
    print(f"  fact_cols: {[c for c, _ in fact_cols]}")
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

        # extract only fact cols that exist
        cols    = [c for c, _ in fact_cols if c in df.columns]
        fact_df = df[cols].copy()

        # cast dtypes
        for col, dtype in fact_cols:
            if col not in fact_df.columns or dtype is None:
                continue
            try:
                if dtype == "TIMESTAMP":
                    fact_df[col] = pd.to_datetime(fact_df[col], errors="coerce")
                else:
                    fact_df[col] = pd.to_numeric(fact_df[col], errors="coerce").astype(dtype)
            except Exception:
                pass

        out_path.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(pa.Table.from_pandas(fact_df), out_path, compression="zstd")
        elapsed = (time.perf_counter() - t0) * 1000
        total  += len(fact_df)
        print(f"  ✓ {f.name}  {len(fact_df):,} rows  {elapsed:.0f}ms")

    print(f"\nDone — {total:,} rows → {out_root}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--in",       dest="in_dir",  default="./data/enriched_fix")
    parser.add_argument("--out",      dest="out_dir",  default="./data/facts_fix")
    parser.add_argument("--entity",   default="trips")
    parser.add_argument("--entities", default="config/entities.yaml")
    parser.add_argument("--force",    action="store_true")
    args = parser.parse_args()
    fix_facts(args.in_dir, args.out_dir, args.entity, args.entities, args.force)


if __name__ == "__main__":
    main()
