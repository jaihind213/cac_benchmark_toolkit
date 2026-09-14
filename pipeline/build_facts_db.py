"""
pipeline/build_facts_db.py
--------------------------
Materialize the facts parquet into a persistent DuckDB table backed by a file
(facts_duck.db). Reads data/facts/entities/<entity>/**/*.parquet once and
INSERTs into a real `facts` table, so benchmark queries hit native DuckDB
storage (compressed, with zonemaps/statistics) instead of scanning parquet
through a view on every run.

Run once after create_facts:
    python -m pipeline.build_facts_db --entity trips
    python -m pipeline.build_facts_db --entity trips --db data/facts_duck.db
    python -m pipeline.build_facts_db --entity trips --threads 8 --memory 12GB

Then point the benchmark at it (the run files ATTACH this DB automatically).
"""

import argparse
import time
from pathlib import Path

import duckdb


def build_facts_db(entity: str, data_dir: str = "./data",
                   db_path: str = None, threads: int = None,
                   memory: str = None, row_group_size: int = None) -> str:
    facts_root = Path(data_dir) / "facts" / "entities" / entity
    if not facts_root.exists():
        raise SystemExit(f"facts not found at {facts_root} — run create_facts first")

    if db_path is None:
        db_path = str(Path(data_dir) / "facts_duck.db")

    facts_glob = str(facts_root / "**" / "*.parquet")
    n_files = sum(1 for _ in facts_root.rglob("*.parquet"))
    src_mb  = sum(f.stat().st_size for f in facts_root.rglob("*.parquet")) / 1e6

    # fresh build — remove any existing db so the table is rebuilt cleanly
    dbp = Path(db_path)
    for p in [dbp, Path(str(dbp) + ".wal")]:
        if p.exists():
            p.unlink()

    print(f"Building facts table for entity={entity}")
    print(f"  source:  {facts_root}  ({n_files} parquet files, {src_mb:.1f}MB)")
    print(f"  target:  {db_path}  (table: facts)")

    con = duckdb.connect(db_path)
    if threads:
        con.execute(f"SET threads={threads}")
    if memory:
        con.execute(f"SET memory_limit='{memory}'")

    t0 = time.perf_counter()
    # CREATE TABLE ... AS reads all parquet and materializes native storage
    con.execute(
        f"CREATE OR REPLACE TABLE facts AS "
        f"SELECT * FROM read_parquet('{facts_glob}', hive_partitioning=false, "
        f"union_by_name=true)"
    )
    n_rows = con.execute("SELECT COUNT(*) FROM facts").fetchone()[0]
    # analyze so the optimizer has fresh statistics/zonemaps
    try:
        con.execute("ANALYZE")
    except Exception:
        pass
    con.execute("CHECKPOINT")   # flush WAL into the main db file
    elapsed = time.perf_counter() - t0

    # report columns + resulting db size
    cols = [r[1] for r in con.execute("PRAGMA table_info('facts')").fetchall()]
    con.close()
    db_mb = dbp.stat().st_size / 1e6 if dbp.exists() else 0.0

    print(f"\n  ✓ facts table built: {n_rows:,} rows in {elapsed:.1f}s")
    print(f"  columns: {cols}")
    print(f"  db size: {db_mb:.1f}MB  (source parquet {src_mb:.1f}MB)")
    print(f"\nBenchmark runs will ATTACH {db_path} and query the facts table.")
    return db_path


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--entity", required=True)
    ap.add_argument("--data-dir", dest="data_dir", default="./data")
    ap.add_argument("--db", dest="db_path", default=None,
                    help="Output DuckDB file (default: <data-dir>/facts_duck.db)")
    ap.add_argument("--threads", type=int, default=None)
    ap.add_argument("--memory", default=None, help="e.g. 12GB")
    args = ap.parse_args()

    build_facts_db(
        entity=args.entity,
        data_dir=args.data_dir,
        db_path=args.db_path,
        threads=args.threads,
        memory=args.memory,
    )


if __name__ == "__main__":
    main()