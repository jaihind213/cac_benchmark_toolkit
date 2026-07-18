"""
benchmark/run.py
----------------
Runs CAC benchmark queries against convolution parquets.

Convolutions loaded from: data/convolutions/entities/<entity>/yyyy/mm/
SQL validation runs on:   data/clean/entities/<entity>/

Usage:
    python -m benchmark.run --benchmark B1 --entity trips
    python -m benchmark.run --benchmark B1 --entity trips --clean ./data
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

def rb_and(a, b): return bytes((BitMap.deserialize(a) & BitMap.deserialize(b)).serialize())
def rb_or(a, b):  return bytes((BitMap.deserialize(a) | BitMap.deserialize(b)).serialize())
def rb_count(a):  return len(BitMap.deserialize(a))
def rb_to_array(a): return list(BitMap.deserialize(a))
def rb_chunk(bitmap_bytes: bytes, chunk_start: int, size: int) -> list:
    """
    Return a chunk of entity_ids from bitmap starting at chunk_start.
    Uses PyRoaring's native iterator — never materialises full bitmap.
    Skips to chunk_start position then collects size elements.
    """
    bm      = BitMap.deserialize(bitmap_bytes)
    result  = []
    current = 0
    for entity_id in bm:              # native C iterator — memory efficient
        if current < chunk_start:
            current += 1
            continue
        result.append(entity_id)
        if len(result) >= size:
            break
    return result
_bitmap_cache = {}

def rb_union_list(bitmaps):
    """Union list of bitmaps using pre-deserialized cache — no repeat deserialization."""
    bm = BitMap()
    for b in bitmaps:
        if b is not None:
            key    = hash(b)
            cached = _bitmap_cache.get(key)
            if cached is None:
                cached = BitMap.deserialize(b)
                _bitmap_cache[key] = cached
            bm |= cached
    return bytes(bm.serialize())

def rb_contains(bitmap_bytes: bytes, entity_id: int) -> bool:
    key = hash(bitmap_bytes)
    bm  = _bitmap_cache.get(key)
    if bm is None:
        bm = BitMap.deserialize(bitmap_bytes)
        _bitmap_cache[key] = bm
    return entity_id in bm


def preload_bitmap_cache(df: pd.DataFrame):
    """Pre-deserialize all bitmaps from a convolution dataframe into cache."""
    if "bitmap" not in df.columns:
        return 0
    count = 0
    for b in df["bitmap"]:
        if b is not None:
            key = hash(b)
            if key not in _bitmap_cache:
                _bitmap_cache[key] = BitMap.deserialize(b)
                count += 1
    return count


def get_connection(threads: int = 4, temp_dir: str = "/tmp/duckdb_spill") -> duckdb.DuckDBPyConnection:
    import os
    os.makedirs(temp_dir, exist_ok=True)
    con = duckdb.connect()
    con.execute(f"SET threads = {threads}")
    con.execute(f"SET temp_directory = '{temp_dir}'")
    for fn, func, args, ret in [
        ("rb_and",        rb_and,        ["BLOB", "BLOB"],            "BLOB"),
        ("rb_or",         rb_or,         ["BLOB", "BLOB"],            "BLOB"),
        ("rb_count",      rb_count,      ["BLOB"],                    "INTEGER"),
        ("rb_to_array",   rb_to_array,   ["BLOB"],                       "BIGINT[]"),
        ("rb_chunk",      rb_chunk,      ["BLOB", "INTEGER", "INTEGER"],  "BIGINT[]"),
        ("rb_contains",   rb_contains,   ["BLOB", "BIGINT"],              "BOOLEAN"),
        ("rb_union_list", rb_union_list, ["BLOB[]"],                      "BLOB"),
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

def load_convolutions(con: duckdb.DuckDBPyConnection, entity: str,
                      data_dir: str = "./data") -> list[str]:
    """
    Load all convolution parquets and fact table for entity into DuckDB.
    - If convolution fits in available memory → load as TABLE (faster queries)
    - Otherwise → create as VIEW (reads from disk on query)
    """
    conv_root = Path(data_dir) / "convolutions" / "entities" / entity
    if not conv_root.exists():
        raise FileNotFoundError(f"Convolutions not found: {conv_root}")

    output_names = set()
    for f in conv_root.rglob("*.parquet"):
        output_names.add(f.stem)

    avail_mb     = get_available_memory_mb()
    tables_loaded   = []
    total_loaded_mb = 0.0
    total_cached    = 0

    for name in sorted(output_names):
        glob_path = str(conv_root / "**" / f"{name}.parquet")
        size_mb   = get_conv_size_mb(conv_root, name)

        # load into memory if fits — use 80% of available memory as threshold
        can_load_memory = (
            avail_mb is not None and
            (total_loaded_mb + size_mb) < avail_mb * 0.8
        )

        try:
            if can_load_memory:
                # load TABLE via DuckDB native reader (fast C++ I/O)
                con.execute(
                    f"CREATE OR REPLACE TABLE {name} AS "
                    f"SELECT * FROM read_parquet('{glob_path}', hive_partitioning=false)"
                )
                total_loaded_mb += size_mb
                # separately pre-deserialize bitmaps into Python cache
                files  = sorted(conv_root.rglob(f"{name}.parquet"))
                df     = pd.concat([pd.read_parquet(f, columns=["bitmap"])
                                    for f in files], ignore_index=True)
                cached = preload_bitmap_cache(df)
                total_cached += cached
                load_type = f"TABLE ({size_mb:.1f}MB, {cached:,} bitmaps pre-cached)"
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
    if facts_root.exists():
        facts_glob = str(facts_root / "**" / "*.parquet")
        facts_size = sum(f.stat().st_size for f in facts_root.rglob("*.parquet")) / 1e6
        try:
            con.execute(
                f"CREATE OR REPLACE VIEW facts AS "
                f"SELECT * FROM read_parquet('{facts_glob}', hive_partitioning=false)"
            )
            tables_loaded.append("facts")
            print(f"  facts: VIEW ({facts_size:.1f}MB on disk)")
        except Exception as e:
            print(f"  warning: could not load facts: {e}")

    if avail_mb:
        print(f"\nMemory: {total_loaded_mb:.1f}MB loaded into RAM, "
              f"{avail_mb:.0f}MB available, "
              f"{total_cached:,} bitmaps pre-deserialized in cache")
    print(f"Loaded {len(tables_loaded)} tables/views")


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
            f"FROM read_parquet('{glob_path}', hive_partitioning=false)"
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

        matches = (sql_sorted == cac_sorted).all().all()
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
    return times[len(times) // 2]


def run_benchmark(benchmark_id: str, entity: str, data_dir: str = "./data",
                  iterations: int = 5, threads: int = 4, memory_limit: str = "4GB",
                  temp_dir: str = "/tmp/duckdb_spill"):

    candidates = list(Path("config/benchmarks").glob(f"{benchmark_id}*.yaml"))
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
    print(f"{'='*60}\n")

    con = get_connection(threads=threads, temp_dir=temp_dir)
    con.execute(f"SET memory_limit = '{memory_limit}'")

    # load all convolutions upfront
    print("Loading convolutions...")
    load_convolutions(con, entity, data_dir)
    print()

    results = []
    print(f"  {'Query':<6} {'Nature':<45} {'CAC Time (ms)':>14} {'Benchmark (ms)':>16}")
    print(f"  {'-'*6} {'-'*45} {'-'*14} {'-'*16}")

    for q in spec["queries"]:
        if not q.get("enabled", True):
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
            "run_at":            datetime.now().isoformat(),
        })

    out_file = RESULTS_DIR / f"{benchmark_id}_cac_times.csv"
    with open(out_file, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=results[0].keys())
        w.writeheader()
        w.writerows(results)
    print(f"\n✓ Results → {out_file}")
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
    args = parser.parse_args()

    for bm_id in args.benchmark:
        run_benchmark(bm_id, args.entity, args.data_dir, args.iterations,
                      args.threads, args.memory, args.temp_dir)


if __name__ == "__main__":
    main()