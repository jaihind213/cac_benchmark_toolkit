"""
benchmark/run.py
----------------
Runs CAC benchmark queries against convolution parquets and reports timings,
speedups vs the published benchmark, and validation against clean source data.

Convolutions loaded from: data/convolutions/entities/<entity>/yyyy/mm/dd/
Facts loaded from:        data/facts/entities/<entity>/
SQL validation runs on:   data/clean/entities/<entity>/

Loading strategy:
  --try-to-cache   load each convolution into a DuckDB TABLE (in RAM) and
                   precompute rb_count for every bitmap into a Python cache,
                   so rb_count becomes a dict lookup instead of a deserialize.
  (default)        create VIEWs that read parquet from disk on each query.
  Only convolutions/facts referenced by the queries being run are loaded.

Query selection:
  --queries Q1 Q3  run only these query IDs (overrides the spec 'enabled' flag).
  (default)        run every query with 'enabled: true' in the spec.

Bitmap UDFs registered into DuckDB: rb_and, rb_or, rb_count, rb_to_array,
rb_chunk, rb_contains, rb_union_list. rb_count is served from a precomputed
count cache when --try-to-cache is set.

Results CSV: results/<B>_cac_times_<threads>threads_<memory>_<cache>.csv

Usage:
    python -m benchmark.run --benchmark B1 --entity trips
    python -m benchmark.run --benchmark B1 --entity trips --duckdb-threads 8 --memory 12GB
    python -m benchmark.run --benchmark B2 --entity trips --queries Q4 Q5 --try-to-cache
"""

import argparse
import csv
import time
from datetime import datetime
from pathlib import Path

import duckdb
import pandas as pd
import yaml
from pyroaring import BitMap

RESULTS_DIR = Path("results")
RESULTS_DIR.mkdir(exist_ok=True)


# ── UDFs ─────────────────────────────────────────────────────────────────────

_bitmap_cache = {}

def _to_bytes(b) -> bytes:
    return b if isinstance(b, bytes) else bytes(b)

def _load_bitmap(b: bytes):
    """Deserialize and cache a bitmap. Call at load time."""
    key = hash(b)
    if key not in _bitmap_cache:
        _bitmap_cache[key] = BitMap.deserialize(b)

def _get_bitmap(b) -> BitMap:
    """Get cached bitmap. Deserializes on miss (handles memoryview from DuckDB)."""
    b   = _to_bytes(b)
    key = hash(b)
    bm  = _bitmap_cache.get(key)
    if bm is None:
        bm = BitMap.deserialize(b)
        _bitmap_cache[key] = bm
    return bm

def rb_and(a, b):   return bytes((_get_bitmap(a) & _get_bitmap(b)).serialize())
def rb_or(a, b):    return bytes((_get_bitmap(a) | _get_bitmap(b)).serialize())
def rb_count(a):    return 0 if a is None else len(_get_bitmap(a))
def rb_to_array(a): return [] if a is None else list(_get_bitmap(a))

def rb_chunk(bitmap_bytes: bytes, chunk_start: int, size: int) -> list:
    if bitmap_bytes is None:
        return []
    bm      = _get_bitmap(bitmap_bytes)
    result  = []
    current = 0
    for entity_id in bm:
        if current < chunk_start:
            current += 1
            continue
        result.append(entity_id)
        if len(result) >= size:
            break
    return result

def rb_union_list(bitmaps):
    bm = BitMap()
    for b in bitmaps:
        if b is not None:
            bm |= _get_bitmap(b)
    return bytes(bm.serialize())

def rb_contains(bitmap_bytes: bytes, entity_id: int) -> bool:
    return False if bitmap_bytes is None else entity_id in _get_bitmap(bitmap_bytes)


# ── Precomputed-column UDF variants ───────────────────────────────────────────
# These accept precomputed bitmap_hash and/or cardinality columns so the UDF
# can skip hashing (use the stored hash as the cache key) or skip counting
# entirely (return the stored cardinality).

def _get_bitmap_h(b, h) -> BitMap:
    """Like _get_bitmap but uses a precomputed hash h as the cache key."""
    bm = _bitmap_cache.get(h)
    if bm is None:
        bm = BitMap.deserialize(_to_bytes(b))
        _bitmap_cache[h] = bm
    return bm

def rb_count_precomputed(bitmap_bytes, bitmap_hash, cardinality):
    """Return the precomputed cardinality directly — no deserialize, no count."""
    return 0 if cardinality is None else int(cardinality)

def rb_count_hash(bitmap_bytes, bitmap_hash):
    """Count via the hash-keyed cache: look up the pre-deserialized bitmap by
    its precomputed hash and return its cardinality. On a cache miss (e.g. when
    --try-to-cache did not preload), deserialize once and store under the hash.
    No per-call hashing of the bitmap bytes."""
    if bitmap_hash is None:
        return 0 if bitmap_bytes is None else len(_get_bitmap(bitmap_bytes))
    return len(_get_bitmap_h(bitmap_bytes, bitmap_hash))

def rb_and_precomputed(a, ha, b, hb):
    """Intersect two bitmaps using precomputed hashes for cache lookup."""
    return bytes((_get_bitmap_h(a, ha) & _get_bitmap_h(b, hb)).serialize())


# ── Pre-computed count cache ──────────────────────────────────────────────────
# Maps hash(bitmap_bytes) → count. Populated at load time.
# rb_count_cached does a dict lookup instead of deserializing.

_count_cache = {}

def preload_counts(df):
    """Precompute rb_count for all bitmaps in a dataframe at load time."""
    n = 0
    for b in df["bitmap"]:
        if b is not None:
            key = hash(_to_bytes(b))
            if key not in _count_cache:
                _count_cache[key] = len(BitMap.deserialize(_to_bytes(b)))
                n += 1
    return n

def preload_bitmaps_by_hash(df):
    """Populate _bitmap_cache keyed by the precomputed bitmap_hash column →
    deserialized BitMap. Lets rb_count_hash / rb_and_precomputed hit the cache
    with zero per-call hashing or deserialization at query time.
    df must have 'bitmap' and 'bitmap_hash' columns."""
    n = 0
    for b, h in zip(df["bitmap"], df["bitmap_hash"]):
        if b is not None and h is not None and h not in _bitmap_cache:
            _bitmap_cache[h] = BitMap.deserialize(_to_bytes(b))
            n += 1
    return n

def rb_count_cached(a):
    """Look up precomputed count — falls back to deserialize on miss."""
    if a is None:
        return 0
    key = hash(_to_bytes(a))
    c   = _count_cache.get(key)
    if c is None:
        c = len(BitMap.deserialize(_to_bytes(a)))
        _count_cache[key] = c
    return c



def get_connection(threads: int = 4, temp_dir: str = "/tmp/duckdb_spill") -> duckdb.DuckDBPyConnection:
    import os
    os.makedirs(temp_dir, exist_ok=True)
    con = duckdb.connect()
    if threads > 1:
        print(f"setting threads...{threads}")
        con.execute(f"SET threads = {threads}")
    con.execute(f"SET temp_directory = '{temp_dir}'")
    for fn, func, args, ret in [
        ("rb_and",        rb_and,        ["BLOB", "BLOB"],            "BLOB"),
        ("rb_or",         rb_or,         ["BLOB", "BLOB"],            "BLOB"),
        ("rb_count",      rb_count_cached, ["BLOB"],                   "BIGINT"),
        ("rb_to_array",   rb_to_array,   ["BLOB"],                       "BIGINT[]"),
        ("rb_chunk",      rb_chunk,      ["BLOB", "INTEGER", "INTEGER"],  "BIGINT[]"),
        ("rb_contains",   rb_contains,   ["BLOB", "BIGINT"],              "BOOLEAN"),
        ("rb_union_list", rb_union_list, ["BLOB[]"],                      "BLOB"),
        ("rb_count_precomputed", rb_count_precomputed,
                          ["BLOB", "BIGINT", "BIGINT"],                   "BIGINT"),
        ("rb_count_hash",  rb_count_hash, ["BLOB", "BIGINT"],             "BIGINT"),
        ("rb_and_precomputed",   rb_and_precomputed,
                          ["BLOB", "BIGINT", "BLOB", "BIGINT"],           "BLOB"),
    ]:
        con.create_function(fn, func, args, ret, null_handling="special")
    return con


def get_conv_size_mb(conv_root: Path, name: str) -> float:
    """Return total size in MB of all parquet files for a convolution."""
    total = sum(f.stat().st_size for f in conv_root.rglob(f"{name}.parquet"))
    return total / 1e6


def get_available_memory_mb() -> float:
    """Return available system memory in MB."""
    try:
        import psutil
        return psutil.virtual_memory().available / 1e6
    except ImportError:
        return None


# ── Load convolutions ─────────────────────────────────────────────────────────

def load_taxi_zones(con: duckdb.DuckDBPyConnection, entity: str,
                    data_dir: str = "./data") -> bool:
    """
    Eagerly build a full in-memory taxi_zones table with an explicit schema and
    row-by-row inserts from the TLC taxi_zone_lookup.csv. No lazy VIEW — the
    table is materialized so B2 Q4/Q5 zone-name joins/subqueries are fast.
    Columns: location_id INT, zone TEXT, borough TEXT, service_zone TEXT.
    """
    zone_csv = Path(data_dir) / "raw" / "entities" / entity / "taxi_zone_lookup.csv"
    if not zone_csv.exists():
        print(f"  taxi_zones: lookup not found at {zone_csv} — skipping")
        return False
    try:
        import csv as _csv
        con.execute("DROP TABLE IF EXISTS taxi_zones")
        con.execute(
            "CREATE TABLE taxi_zones ("
            "  location_id  INTEGER,"
            "  zone         VARCHAR,"
            "  borough      VARCHAR,"
            "  service_zone VARCHAR"
            ")"
        )
        with open(zone_csv, newline="") as f:
            reader = _csv.DictReader(f)
            def pick(row, *names):
                for n in names:
                    if n in row and row[n] not in (None, ""):
                        return row[n]
                return None
            batch = []
            for row in reader:
                loc = pick(row, "LocationID", "location_id")
                batch.append((
                    int(loc) if loc not in (None, "") else None,
                    pick(row, "Zone", "zone"),
                    pick(row, "Borough", "borough"),
                    pick(row, "service_zone", "Service_Zone", "ServiceZone"),
                ))
        con.executemany(
            "INSERT INTO taxi_zones VALUES (?, ?, ?, ?)", batch
        )
        nz = con.execute("SELECT COUNT(*) FROM taxi_zones").fetchone()[0]
        print(f"  taxi_zones: TABLE ({nz} zones, in-memory, {len(batch)} rows inserted)")
        return True
    except Exception as e:
        print(f"  warning: could not load taxi_zones: {e}")
        return False


def load_convolutions(con: duckdb.DuckDBPyConnection, entity: str,
                      data_dir: str = "./data",
                      try_to_cache: bool = True,
                      only_tables: set = None) -> list[str]:
    """
    Load convolution parquets and fact table for entity into DuckDB.
    - If try_to_cache and it fits in available memory → load as TABLE and
      precompute rb_count for every bitmap into the count cache.
    - Otherwise → create as VIEW (reads from disk on query).
    only_tables: None → load all; empty set → load none; set → load those.
    """
    conv_root = Path(data_dir) / "convolutions" / "entities" / entity
    if not conv_root.exists():
        raise FileNotFoundError(f"Convolutions not found: {conv_root}")

    output_names = set()
    for f in conv_root.rglob("*.parquet"):
        output_names.add(f.stem)

    # filter to only needed tables — None means load all, empty set means load none
    if only_tables is not None:
        output_names = {n for n in output_names if n in only_tables}

    avail_mb     = get_available_memory_mb()
    tables_loaded = []
    total_loaded_mb = 0.0

    for name in sorted(output_names):
        glob_path = str(conv_root / "**" / f"{name}.parquet")
        size_mb   = get_conv_size_mb(conv_root, name)

        # load into memory if fits — use 80% of available memory as threshold
        can_load_memory = try_to_cache and (
            avail_mb is not None and
            (total_loaded_mb + size_mb) < avail_mb * 0.8
        )

        try:
            if can_load_memory:
                con.execute(
                    f"CREATE OR REPLACE TABLE {name} AS "
                    f"SELECT * FROM read_parquet('{glob_path}', hive_partitioning=false)"
                )
                total_loaded_mb += size_mb

                # detect precomputed columns in this convolution
                tbl_cols = {r[1] for r in
                            con.execute(f"PRAGMA table_info('{name}')").fetchall()}
                has_card = "cardinality" in tbl_cols
                has_hash = "bitmap_hash" in tbl_cols

                import pandas as pd
                files = sorted(conv_root.rglob(f"{name}.parquet"))

                if has_hash:
                    # preload deserialized bitmaps keyed by the stored hash, so
                    # rb_count_hash / rb_and_precomputed are pure cache hits
                    n_bm = 0
                    for cf in files:
                        dfc = pd.read_parquet(cf, columns=["bitmap", "bitmap_hash"])
                        n_bm += preload_bitmaps_by_hash(dfc)
                    load_type = f"TABLE ({size_mb:.1f}MB, {n_bm:,} bitmaps cached by hash)"
                elif has_card:
                    # cardinality precomputed on disk → no need to build count cache
                    load_type = f"TABLE ({size_mb:.1f}MB, cardinality precomputed)"
                else:
                    # no precomputed cols → build the count cache from bitmaps
                    n_cached = 0
                    for cf in files:
                        dfc = pd.read_parquet(cf, columns=["bitmap"])
                        n_cached += preload_counts(dfc)
                    load_type = f"TABLE ({size_mb:.1f}MB, {n_cached:,} counts cached)"
                if has_card and has_hash:
                    load_type += " +cardinality"
            else:
                con.execute(
                    f"CREATE OR REPLACE VIEW {name} AS "
                    f"SELECT * FROM read_parquet('{glob_path}', hive_partitioning=false)"
                )
                load_type = f"VIEW ({size_mb:.1f}MB on disk)"

            tables_loaded.append(name)
            print(f"  {name}: {load_type}")
        except Exception as e:
            print(f"  warning: could not load {name}: {e}")

    # load facts table — partitioned by yyyy/mm/dd
    facts_root = Path(data_dir) / "facts" / "entities" / entity
    if not facts_root.exists():
        # fallback to facts_fix
        facts_root = Path(data_dir) / "facts_fix" / "entities" / entity
    load_facts = (only_tables is None) or ("facts" in only_tables)
    if load_facts and facts_root.exists():
        facts_glob = str(facts_root / "**" / "*.parquet")
        facts_size = sum(f.stat().st_size for f in facts_root.rglob("*.parquet")) / 1e6

        # inspect one facts file to report row-group size + compression
        try:
            import pyarrow.parquet as _pq
            sample = next(facts_root.rglob("*.parquet"))
            md     = _pq.ParquetFile(sample).metadata
            n_rg   = md.num_row_groups
            rg0    = md.row_group(0)
            rg_rows = rg0.num_rows
            codec   = rg0.column(0).compression
            print(f"  facts parquet: {rg_rows:,} rows/group, {n_rg} groups in sample, "
                  f"compression={codec}")
        except Exception as e:
            print(f"  (could not read facts parquet metadata: {e})")

        try:
            con.execute(
                f"CREATE OR REPLACE VIEW facts AS "
                f"SELECT * FROM read_parquet('{facts_glob}', hive_partitioning=false)"
            )
            tables_loaded.append("facts")
            print(f"  facts: VIEW ({facts_size:.1f}MB on disk)")
        except Exception as e:
            print(f"  warning: could not load facts: {e}")

    # eagerly materialize taxi_zones (full in-memory table, explicit inserts)
    load_taxi_zones(con, entity, data_dir)

    if avail_mb:
        print(f"\nMemory: {total_loaded_mb:.1f}MB loaded into RAM, "
              f"{avail_mb:.0f}MB available")
    print(f"Loaded {len(tables_loaded)} tables/views")
    return tables_loaded


# ── Validation ────────────────────────────────────────────────────────────────

def _fmt_table(df: pd.DataFrame, max_rows: int = 20) -> str:
    """Format a DataFrame as a bordered table string."""
    if df is None or df.empty:
        return "         (empty)"
    display = df.head(max_rows)
    cols    = list(display.columns)
    widths  = [max(len(str(c)), max(len(str(v)) for v in display[c])) for c in cols]
    sep     = "         +" + "+".join("-" * (w + 2) for w in widths) + "+"
    header  = "         |" + "|".join(f" {c:<{w}} " for c, w in zip(cols, widths)) + "|"
    rows    = []
    for _, row in display.iterrows():
        rows.append("         |" + "|".join(f" {str(row[c]):<{w}} " for c, w in zip(cols, widths)) + "|")
    lines = [sep, header, sep] + rows + [sep]
    if len(df) > max_rows:
        lines.append(f"         ... {len(df) - max_rows} more rows")
    return "\n".join(lines)


def validate_query(con: duckdb.DuckDBPyConnection, q: dict,
                   entity: str, data_dir: str) -> dict:
    validation = q.get("validation")
    if not validation:
        return {"status": "skipped", "reason": "no validation config"}

    compare = validation.get("compare", "exact")
    if compare == "none":
        return {"status": "skipped", "reason": "compare: none"}

    sql     = q.get("sql", "").strip()
    cac_sql = q.get("cac_sql", "").strip()

    try:
        # use clean data for validation — same data convolutions are built from
        for subdir in ["clean", "enriched_fix", "enriched"]:
            data_path = Path(data_dir) / subdir / "entities" / entity
            if data_path.exists():
                break

        glob_path   = str(data_path / "**" / "*.parquet")
        sql_on_data = sql.replace(
            "FROM trips",
            f"FROM read_parquet('{glob_path}', hive_partitioning=false, union_by_name=true)"
        )
        sql_result  = con.execute(sql_on_data).fetchdf()
        cac_result  = con.execute(cac_sql).fetchdf()

        if compare == "count_only":
            match = len(sql_result) == len(cac_result)
            return {
                "status":   "✓ MATCH" if match else "✗ MISMATCH",
                "compare":  "row count",
                "sql_rows": len(sql_result),
                "cac_rows": len(cac_result),
            }

        # exact — sort both and compare row by row col by col
        sql_key_cols = list(sql_result.columns[:-1])
        cac_key_cols = list(cac_result.columns[:-1])
        sql_val_col  = sql_result.columns[-1]

        sql_sorted = sql_result.sort_values(sql_key_cols).reset_index(drop=True)
        cac_sorted = cac_result.sort_values(cac_key_cols).reset_index(drop=True)

        # rename cac cols to match sql col names
        rename_map = dict(zip(cac_result.columns, sql_result.columns))
        cac_sorted = cac_sorted.rename(columns=rename_map)

        # NaN-safe: fill NaN in key (non-value) columns with a sentinel so the
        # NULL group (e.g. NULL zone from a LEFT JOIN) sorts to a stable position
        # and compares equal. Without this, NaN == NaN is False and a matching
        # NULL row is falsely reported as a mismatch.
        key_cols = list(sql_sorted.columns[:-1])
        for col in key_cols:
            if col in sql_sorted.columns:
                sql_sorted[col] = sql_sorted[col].fillna("__NULL__")
            if col in cac_sorted.columns:
                cac_sorted[col] = cac_sorted[col].fillna("__NULL__")
        sql_sorted = sql_sorted.sort_values(key_cols).reset_index(drop=True)
        cac_sorted = cac_sorted.sort_values(key_cols).reset_index(drop=True)

        # round float columns to 2dp before comparison
        for col in sql_sorted.columns:
            if sql_sorted[col].dtype in ("float32", "float64"):
                sql_sorted[col] = sql_sorted[col].round(2)
                if col in cac_sorted.columns:
                    cac_sorted[col] = cac_sorted[col].round(2)

        # normalise dtypes — cast float to int where values are whole numbers
        for col in sql_sorted.columns:
            if col not in cac_sorted.columns:
                continue
            try:
                sql_numeric = pd.to_numeric(sql_sorted[col], errors="coerce").dropna()
                cac_numeric = pd.to_numeric(cac_sorted[col], errors="coerce").dropna()
                # only cast if column has numeric values AND all are whole numbers
                if len(sql_numeric) > 0 and len(cac_numeric) > 0 and \
                   (sql_numeric % 1 == 0).all() and (cac_numeric % 1 == 0).all():
                    sql_sorted[col] = pd.to_numeric(sql_sorted[col], errors="coerce").astype("Int64")
                    cac_sorted[col] = pd.to_numeric(cac_sorted[col], errors="coerce").astype("Int64")
            except Exception:
                pass

        sql_total = int(sql_sorted[sql_val_col].sum())
        cac_total = int(cac_sorted[sql_val_col].sum()) if sql_val_col in cac_sorted.columns else 0

        if len(sql_sorted) != len(cac_sorted):
            return {
                "status":     "✗ MISMATCH",
                "compare":    "exact (row×col)",
                "reason":     f"row count differs: sql={len(sql_sorted)} cac={len(cac_sorted)}",
                "sql_total":  sql_total,
                "cac_total":  cac_total,
                "sql_result": sql_sorted,
                "cac_result": cac_sorted,
            }

        if list(sql_sorted.columns) != list(cac_sorted.columns):
            matches = False
        else:
            eq = (sql_sorted.values == cac_sorted.values)
            matches = bool(eq.all())
        return {
            "status":     "✓ MATCH" if matches else "✗ MISMATCH",
            "compare":    "exact (row×col)",
            "sql_total":  sql_total,
            "cac_total":  cac_total,
            "sql_result": sql_sorted,
            "cac_result": cac_sorted,
        }

    except Exception as e:
        return {"status": "ERROR", "reason": str(e)}


# ── Query execution ───────────────────────────────────────────────────────────

def run_query(con: duckdb.DuckDBPyConnection, sql: str, iterations: int = 5) -> float:
    """Run query N times. Return median time in ms."""
    times = []
    for _ in range(iterations):
        t0 = time.perf_counter()
        con.execute(sql).fetchall()
        times.append((time.perf_counter() - t0) * 1000)
    times.sort()
    print(f"    {iterations} iterations: {', '.join(f'{t:.2f}ms' for t in times)}")
    return times[len(times) // 2]


def run_benchmark(benchmark_id: str, entity: str, data_dir: str = "./data",
                  iterations: int = 5, threads: int = 4, memory_limit: str = "4GB",
                  temp_dir: str = "/tmp/duckdb_spill", try_to_cache: bool = False,
                  only_queries: list = None):
    candidates = [
        p for p in Path("config/benchmarks").glob("*.yaml")
        if p.stem == benchmark_id
    ]
    if not candidates:
        raise FileNotFoundError(f"No spec found for {benchmark_id} in config/benchmarks/")

    with open(candidates[0]) as f:
        spec = yaml.safe_load(f)

    print(f"\n{'='*60}")
    print(f"Running benchmark: {spec['benchmark_id']} — {spec['name']}")
    print(f"Entity:     {entity}")
    print(f"Threads:    {threads}")
    print(f"Memory:     {memory_limit}")
    print(f"Iterations: {iterations}")
    print(f"Cache:      {'enabled' if try_to_cache else 'disabled'}")
    print(f"{'='*60}\n")

    con = get_connection(threads=threads, temp_dir=temp_dir)
    con.execute(f"SET memory_limit = '{memory_limit}'")

    # determine which convolutions/facts are needed
    import re
    only_set = set(only_queries) if only_queries else None
    needed_tables = set()
    for q in spec["queries"]:
        # if --queries given, scan those regardless of enabled; else respect enabled
        if only_set:
            if q["id"] not in only_set:
                continue
        elif not q.get("enabled", True):
            continue
        cac_sql = q.get("cac_sql", "")
        for m in re.finditer(r'FROM\s+(conv_\w+|facts)', cac_sql, re.IGNORECASE):
            needed_tables.add(m.group(1))

    print(f"Loading convolutions needed by enabled queries: {sorted(needed_tables)}")
    cached_tables = load_convolutions(con, entity, data_dir,
                                      try_to_cache=try_to_cache,
                                      only_tables=needed_tables)
    print()

    results = []
    print(f"  {'Query':<6} {'Nature':<45} {'CAC Time (ms)':>14} {'Benchmark (ms)':>16}")
    print(f"  {'-'*6} {'-'*45} {'-'*14} {'-'*16}")

    for q in spec["queries"]:
        # --queries overrides enabled; without it, respect enabled flag
        if only_set:
            if q["id"] not in only_set:
                print(f"  {'skipped':<6} {q['id']} — not in --queries")
                continue
        elif not q.get("enabled", True):
            print(f"  {'skipped':<6} {q['id']} — disabled in spec")
            continue

        qid     = q["id"]
        sql     = q.get("cac_sql") or q["sql"]
        nature  = q["nature"]
        bm_time = q.get("benchmark_time_ms")

        try:
            cac_time = run_query(con, sql, iterations)
            speedup  = f"{bm_time/cac_time:.1f}x" if bm_time else "N/A"
            print(f"  {qid:<6} {nature[:45]:<45} {cac_time:>13.2f}ms "
                  f"{str(bm_time)+'ms' if bm_time else 'N/A':>15}  speedup={speedup}")

            # expected results check
            expected = q.get("expected_results")
            if expected:
                result_rows = con.execute(sql).fetchall()
                result_dict = {str(r[0]): r[1] for r in result_rows}
                for k, v in expected.items():
                    actual = result_dict.get(str(k), 0)
                    status = "✓" if actual >= v else f"✗ expected >={v:,} got {actual:,}"
                    print(f"         {k}: {actual:,}  {status}")

            # validation
            if q.get("validation"):
                val = validate_query(con, q, entity, data_dir)
                if val["status"] == "skipped":
                    print(f"         validation: skipped — {val.get('reason','')}")
                elif val["status"] == "ERROR":
                    print(f"         validation: ERROR — {val.get('reason','')}")
                elif "✓" in val["status"]:
                    # match — print result inline
                    print(f"         validation: {val['status']}")
                    if "sql_result" in val:
                        print(_fmt_table(val["sql_result"]))
                else:
                    # mismatch — save to file
                    print(f"         validation: {val['status']}")
                    if "sql_result" in val and "cac_result" in val:
                        reports_dir = Path("reports")
                        reports_dir.mkdir(exist_ok=True)
                        mismatch_file = reports_dir / f"mismatch_{benchmark_id}_{qid}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
                        with open(mismatch_file, "w") as mf:
                            mf.write(f"Benchmark: {benchmark_id}  Query: {qid}\n")
                            mf.write(f"Status: {val['status']}\n\n")
                            mf.write("SQL result:\n")
                            mf.write(_fmt_table(val["sql_result"]) + "\n\n")
                            mf.write("CAC result:\n")
                            mf.write(_fmt_table(val["cac_result"]) + "\n")
                        print(f"         mismatch details → {mismatch_file}")

        except Exception as e:
            cac_time = None
            print(f"  {qid:<6} ERROR: {e}")

        results.append({
            "benchmark_id":      spec["benchmark_id"],
            "query_id":          qid,
            "nature":            nature,
            "cac_phase":         q.get("cac_phase", ""),
            "cac_time_ms":       round(cac_time, 3) if cac_time else "",
            "benchmark_time_ms": bm_time or "",
            "speedup":           f"{bm_time/cac_time:.1f}x" if (bm_time and cac_time) else "",
            "iterations":        iterations,
            "threads":           threads,
            "memory_limit":      memory_limit,
            "try_to_cache":      try_to_cache,
            "cached_tables":     ",".join(cached_tables) if cached_tables else "",
            "run_at":            datetime.now().isoformat(),
        })

    cache_suffix = "cache_enabled" if try_to_cache else "cache_disabled"
    out_file = RESULTS_DIR / f"{benchmark_id}_cac_times_{threads}threads_{memory_limit}_{cache_suffix}.csv"
    with open(out_file, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=results[0].keys())
        w.writeheader()
        w.writerows(results)
    print(f"\n✓ Results → {out_file}")

    # reprint the results table (2nd time) as a clean summary
    print(f"\n{'='*80}")
    print(f"SUMMARY — {spec['benchmark_id']}  ({threads} threads, {memory_limit}, "
          f"cache {'on' if try_to_cache else 'off'})")
    print(f"{'='*80}")
    print(f"  {'Query':<6} {'CAC (ms)':>12} {'Benchmark (ms)':>16} {'Speedup':>10}")
    print(f"  {'-'*6} {'-'*12} {'-'*16} {'-'*10}")
    for r in results:
        cac = r["cac_time_ms"]
        bm  = r["benchmark_time_ms"]
        sp  = r["speedup"] or "—"
        cac_s = f"{cac:>12}" if cac != "" else f"{'ERROR':>12}"
        bm_s  = f"{bm:>16}" if bm != "" else f"{'—':>16}"
        print(f"  {r['query_id']:<6} {cac_s} {bm_s} {sp:>10}")
    print(f"{'='*80}")

    return results


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--benchmark",  nargs="+", required=True)
    parser.add_argument("--entity",     required=True)
    parser.add_argument("--data-dir",   dest="data_dir", default="./data")
    parser.add_argument("--iterations",    type=int, default=5)
    parser.add_argument("--duckdb-threads", dest="threads", type=int, default=4,
                        help="DuckDB thread count (default: 4)")
    parser.add_argument("--memory",         default="4GB",
                        help="DuckDB memory limit (default: 4GB)")
    parser.add_argument("--temp-dir",        dest="temp_dir", default="/tmp/duckdb_spill",
                        help="DuckDB spill-to-disk directory (default: /tmp/duckdb_spill)")
    parser.add_argument("--try-to-cache",    dest="try_to_cache", action="store_true",
                        default=False,
                        help="Load convolutions into memory as TABLE + precompute counts (default: False)")
    parser.add_argument("--no-cache",        dest="try_to_cache", action="store_false",
                        help="Force VIEW mode — read convolutions from disk")
    parser.add_argument("--queries",         nargs="+", default=None,
                        help="Only run specific query IDs, e.g. --queries Q1 Q3 (overrides 'enabled')")
    args = parser.parse_args()

    for bm_id in args.benchmark:
        run_benchmark(bm_id, args.entity, args.data_dir, args.iterations,
                      args.threads, args.memory, args.temp_dir, args.try_to_cache,
                      only_queries=args.queries)


if __name__ == "__main__":
    main()