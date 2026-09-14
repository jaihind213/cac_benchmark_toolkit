"""
pipeline/enrich.py
------------------
Assigns integer entity_id via Postgres BIGSERIAL.

Reads from:  data/extracted/entities/<entity>/
Writes to:   data/enriched/entities/<entity>/

For each file:
  1. Insert trip_id into Postgres cac.trip_ids → get integer entity_id back
  2. Attach entity_id to dataframe
  3. Write enriched parquet

Usage:
    python -m pipeline.enrich --entity trips --pg-dsn $PG_DSN
    python -m pipeline.enrich --entity trips --pg-dsn $PG_DSN --years 2009-2015
"""

import argparse
import csv
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
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


def ensure_schema(conn):
    with conn.cursor() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS cac")
    conn.commit()


def ensure_id_table(conn, id_table: str):
    with conn.cursor() as cur:
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {id_table} (
                trip_id   TEXT   PRIMARY KEY,
                entity_id BIGINT GENERATED ALWAYS AS IDENTITY
            )
        """)
    conn.commit()


def insert_and_get_ids(conn, id_table: str, trip_ids: list[str]) -> dict[str, int]:
    """
    Batch insert trip_ids and return mapping trip_id → entity_id.
    Inserts in pages of 10K.
    """
    mapping   = {}
    page_size = 10_000

    with conn.cursor() as cur:
        for i in range(0, len(trip_ids), page_size):
            batch = trip_ids[i:i + page_size]
            rows  = execute_values(
                cur,
                f"""INSERT INTO {id_table} (trip_id) VALUES %s
                    ON CONFLICT (trip_id) DO UPDATE SET trip_id = EXCLUDED.trip_id
                    RETURNING trip_id, entity_id""",
                [(t,) for t in batch],
                fetch=True,
                page_size=page_size,
            )
            for trip_id, entity_id in rows:
                mapping[trip_id] = entity_id

    conn.commit()
    return mapping


def enrich_file(path: Path, out_path: Path, entity_cfg: dict, conn) -> int:
    df          = pd.read_parquet(path)
    entity_id   = entity_cfg.get("entity_id", {})
    id_table    = entity_id.get("id_table", "cac.trip_ids")
    unique_id   = entity_cfg.get("unique_id", {})
    trip_id_col = unique_id.get("col", "trip_id")

    if trip_id_col not in df.columns:
        raise ValueError(
            f"Column '{trip_id_col}' not found in {path}. "
            "Run pipeline/extract_id.py first."
        )

    ensure_id_table(conn, id_table)

    trip_ids = df[trip_id_col].tolist()
    mapping  = insert_and_get_ids(conn, id_table, trip_ids)

    df["entity_id"] = df[trip_id_col].map(mapping).astype("Int64")

    out_path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pandas(df), out_path, compression="zstd")
    return len(df)


def enrich(entity: str, entities_yaml: str, pg_dsn: str,
           data_dir: str = "./data", years: list[int] = None):

    entity_cfg   = load_entity_config(entities_yaml, entity)
    files        = paths_for_entity(data_dir + "/extracted", entity, years)
    extract_root = entity_dir(data_dir + "/extracted", entity)

    conn = psycopg2.connect(pg_dsn)
    ensure_schema(conn)

    enriched_root = entity_dir(data_dir + "/enriched", entity)

    print(f"Enriching entity={entity}  ({len(files)} files)")

    total_rows = 0
    total_ms   = 0.0
    file_times = []
    n_files    = len(files)

    for i, f in enumerate(files, 1):
        rel      = f.relative_to(extract_root)
        out_path = enriched_root / rel

        t0 = time.perf_counter()
        with TimerLog("enrich", scope=f.name) as t:
            rows = enrich_file(f, out_path, entity_cfg, conn)
            t.rows = rows
        elapsed_ms = (time.perf_counter() - t0) * 1000

        total_rows += rows
        total_ms   += elapsed_ms
        file_times.append((f.name, rows, elapsed_ms))
        print(f"  ✓ [{i}/{n_files}] {f.name}  {rows:,} rows  {elapsed_ms:.0f}ms")

    conn.close()

    if file_times:
        print(f"\n{'='*60}")
        print(f"TOTAL REPORT — entity={entity}")
        print(f"{'='*60}")
        processed_years = sorted(set(f.parent.name for f in files))
        print(f"  Years:       {', '.join(str(y) for y in processed_years)}")
        print(f"  Files:       {len(file_times):>12,}")
        print(f"  Total rows:  {total_rows:>12,}")
        print(f"  Total time:  {total_ms/1000:>11.1f}s")
        print(f"  Avg/file:    {total_ms/len(file_times):>10.0f}ms")
        print(f"{'='*60}")

        reports_dir = Path("reports")
        reports_dir.mkdir(exist_ok=True)
        report_file = reports_dir / f"enrich_report_{entity}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        with open(report_file, "w", newline="") as rf:
            w = csv.DictWriter(rf, fieldnames=["entity", "file", "rows", "elapsed_ms"])
            w.writeheader()
            w.writerows([
                {"entity": entity, "file": name, "rows": rows, "elapsed_ms": round(ms)}
                for name, rows, ms in file_times
            ])
        print(f"\nReport → {report_file}")

    print(f"\nEnriched → {enriched_root}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--entity",   required=True)
    parser.add_argument("--entities", default="config/entities.yaml")
    parser.add_argument("--pg-dsn",   dest="pg_dsn", required=True)
    parser.add_argument("--years",    type=str, nargs="+", default=None)
    parser.add_argument("--data-dir", dest="data_dir", default="./data")
    args  = parser.parse_args()
    years = parse_years(args.years) if args.years else None

    enrich(
        entity=args.entity,
        entities_yaml=args.entities,
        pg_dsn=args.pg_dsn,
        data_dir=args.data_dir,
        years=years,
    )


if __name__ == "__main__":
    main()