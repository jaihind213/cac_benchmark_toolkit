"""
pipeline/convolute.py
---------------------
Builds bitmap convolutions from clean parquet files.

Reads from:  data/clean/entities/<entity>/
Writes to:   data/convolutions/entities/<entity>/yyyy/mm/dd/<name>.parquet
             (partition path driven by output.partitions spec)

Schema of each output parquet:
  <dim output_cols...>   e.g. pickup_date DATE, cab_type STRING
  bitmap                 BLOB — roaring bitmap of entity_ids

Each convolution spec in convolutions.yaml supports:
  enabled: true|false    optional, default true — skip building when false
  dim_cols               list of {col, transform, output_col}
  output.partitions      folder partition spec, e.g. [year, month, day]

Usage:
    python -m pipeline.convolute --entity trips
    python -m pipeline.convolute --entity trips --years 2009-2015
    python -m pipeline.convolute --entity trips --only conv_cab_type conv_pickup_zone
    python -m pipeline.convolute --entity trips --row-group-size 500000
"""

import argparse
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import yaml
from pyroaring import BitMap

from pipeline.timer import TimerLog, parse_years, entity_dir, paths_for_entity


def stable_hash(b: bytes) -> int:
    """Deterministic 64-bit hash of bitmap bytes (stable across processes).
    Uses blake2b so precomputed hashes match at query time regardless of
    PYTHONHASHSEED. Returned as a signed 64-bit int to fit DuckDB BIGINT."""
    import hashlib
    h = hashlib.blake2b(b, digest_size=8).digest()
    val = int.from_bytes(h, "big", signed=False)
    # map to signed 64-bit range for DuckDB BIGINT
    return val - (1 << 64) if val >= (1 << 63) else val


# ── Transforms ────────────────────────────────────────────────────────────────

TRANSFORM_REGISTRY = {
    "none":         lambda s: s,
    "round":        lambda s: (pd.to_numeric(s, errors="coerce") + 0.5).apply(
                        lambda x: int(x) if pd.notna(x) else pd.NA
                    ).astype("Int64"),
    "round_1dp":    lambda s: pd.to_numeric(s, errors="coerce").round(1),
    "floor":        lambda s: np.floor(pd.to_numeric(s, errors="coerce")).astype("Int64"),
    "ceiling":      lambda s: np.ceil(pd.to_numeric(s, errors="coerce")).astype("Int64"),
    "extract_date": lambda s: pd.to_datetime(s, errors="coerce").dt.date,
    "extract_hour": lambda s: pd.to_datetime(s, errors="coerce").dt.hour,
    "extract_dow":  lambda s: pd.to_datetime(s, errors="coerce").dt.day_name(),
    "extract_year": lambda s: pd.to_datetime(s, errors="coerce").dt.year,
    "extract_month":lambda s: pd.to_datetime(s, errors="coerce").dt.month,
}


def apply_transform(series: pd.Series, transform: str) -> pd.Series:
    fn = TRANSFORM_REGISTRY.get(transform)
    if fn is None:
        raise ValueError(f"Unknown transform: '{transform}'. Available: {list(TRANSFORM_REGISTRY)}")
    return fn(series)


# ── Partition path ─────────────────────────────────────────────────────────────

def get_partition_path(row_date, parts: list[str]) -> str:
    """
    Build partition folder path from a date value and parts spec.
    parts: [year], [year,month], [year,month,day]
    """
    d = pd.Timestamp(row_date)
    segments = []
    for part in parts:
        if part == "year":
            segments.append(f"{d.year:04d}")
        elif part == "month":
            segments.append(f"{d.month:02d}")
        elif part == "day":
            segments.append(f"{d.day:02d}")
    return "/".join(segments)


# ── Build convolution ─────────────────────────────────────────────────────────

def build_convolution(df: pd.DataFrame, conv: dict) -> dict[str, pd.DataFrame] | None:
    """
    Build bitmap convolution for one spec.
    Returns dict of {partition_path: DataFrame} or None if columns missing.

    Each output DataFrame has dim output_cols + bitmap column.
    Grouped by partition_path so each parquet covers one time partition.
    """
    dim_cols   = conv["dim_cols"]
    output_cfg = conv["output"]
    bitmap_col = output_cfg["bitmap_col"]
    partitions = output_cfg.get("partitions", [])
    add_hash   = output_cfg.get("add_hash_col", False)
    add_card   = output_cfg.get("add_cardinality_col", False)
    keep_null  = output_cfg.get("keep_null_dims", False)

    # check required source columns exist
    missing = [d["col"] for d in dim_cols if d["col"] not in df.columns]
    if missing:
        # when keep_null_dims is set, a missing dim column is treated as an
        # all-NULL column rather than skipping the file. This lets rows from
        # years where the column doesn't exist (e.g. 2009-2010 PULocationID)
        # still contribute — they group under a NULL dimension value, which a
        # downstream LEFT JOIN maps to a NULL bucket (matches Altinity Q4).
        if keep_null:
            df = df.copy()
            for m in missing:
                df[m] = pd.NA
            print(f"    {conv['name']}: columns {missing} absent — filled as NULL (keep_null_dims)")
        else:
            print(f"    skipping {conv['name']} — missing columns: {missing}")
            return None

    # build work dataframe with transformed columns
    work      = pd.DataFrame()
    col_names = []

    # track which output_col is the partition column
    partition_col = partitions[0]["col"] if partitions else None
    partition_parts = partitions[0]["parts"] if partitions else []

    # handle duplicate source cols (e.g. pickup_datetime used twice)
    seen_sources = {}
    for dim in dim_cols:
        col       = dim["col"]
        transform = dim.get("transform", "none")
        out_col   = dim.get("output_col", col)

        # avoid re-transforming same source col twice
        cache_key = (col, transform)
        if cache_key not in seen_sources:
            seen_sources[cache_key] = apply_transform(df[col], transform)
        work[out_col] = seen_sources[cache_key]
        col_names.append(out_col)

    work[bitmap_col] = df[bitmap_col]
    work = work.dropna(subset=[bitmap_col])

    if work.empty:
        return None

    # group by partition column values
    results = {}

    if partition_col and partition_col in work.columns:
        for part_val, part_grp in work.groupby(partition_col, sort=True):
            part_path = get_partition_path(part_val, partition_parts)

            # within partition, group by remaining dim cols to build bitmaps
            group_cols = [c for c in col_names if c != partition_col]
            rows = []

            if group_cols:
                for keys, grp in part_grp.groupby(group_cols, sort=True, dropna=not keep_null):
                    bm       = BitMap(grp[bitmap_col].dropna().astype(int).tolist())
                    bm_bytes = bytes(bm.serialize())
                    row = dict(zip(group_cols, keys if isinstance(keys, tuple) else (keys,)))
                    row[partition_col] = part_val
                    row["bitmap"] = bm_bytes
                    if add_card: row["cardinality"] = len(bm)
                    if add_hash: row["bitmap_hash"] = stable_hash(bm_bytes)
                    rows.append(row)
            else:
                bm       = BitMap(part_grp[bitmap_col].dropna().astype(int).tolist())
                bm_bytes = bytes(bm.serialize())
                row = {partition_col: part_val, "bitmap": bm_bytes}
                if add_card: row["cardinality"] = len(bm)
                if add_hash: row["bitmap_hash"] = stable_hash(bm_bytes)
                rows.append(row)

            if rows:
                extra    = (["cardinality"] if add_card else []) + \
                           (["bitmap_hash"] if add_hash else [])
                out_cols = [partition_col] + group_cols + ["bitmap"] + extra
                new_df   = pd.DataFrame(rows)[out_cols]
                # accumulate multiple days that share the same partition path
                if part_path in results:
                    results[part_path] = pd.concat(
                        [results[part_path], new_df], ignore_index=True
                    )
                else:
                    results[part_path] = new_df
    else:
        # no partitioning — one output file
        rows = []
        for keys, grp in work.groupby(col_names, sort=True, dropna=not keep_null):
            bm       = BitMap(grp[bitmap_col].dropna().astype(int).tolist())
            bm_bytes = bytes(bm.serialize())
            row = dict(zip(col_names, keys if isinstance(keys, tuple) else (keys,)))
            row["bitmap"] = bm_bytes
            if add_card: row["cardinality"] = len(bm)
            if add_hash: row["bitmap_hash"] = stable_hash(bm_bytes)
            rows.append(row)
        if rows:
            results[""] = pd.DataFrame(rows)

    return results if results else None


# ── File processing ───────────────────────────────────────────────────────────

def convolute_file(path: Path, conv_root: Path, conv_specs: list[dict],
                   entity: str, row_group_size: int = 100_000):
    df = pd.read_parquet(path)

    for conv in conv_specs:
        if conv.get("entity", entity) != entity:
            continue

        # read the same keep_null_dims flag build_convolution uses, so the
        # re-aggregate path below groups NULL dim keys consistently
        keep_null = conv.get("output", {}).get("keep_null_dims", False)

        result_map = build_convolution(df, conv)
        if result_map is None:
            continue

        for part_path, result_df in result_map.items():
            out_dir = conv_root / part_path if part_path else conv_root
            out_dir.mkdir(parents=True, exist_ok=True)
            out_file = out_dir / f"{conv['name']}.parquet"

            # normalise pickup_date to string for consistent schema across files
            if "pickup_date" in result_df.columns:
                result_df["pickup_date"] = result_df["pickup_date"].astype(str)

            # sort by dim cols (exclude bitmap + precomputed cols) for compression
            precomputed = {"bitmap", "cardinality", "bitmap_hash"}
            dim_cols_sorted = [c for c in result_df.columns if c not in precomputed]
            result_df = result_df.sort_values(dim_cols_sorted).reset_index(drop=True)

            # append if file exists (multiple source files may cover same partition)
            if out_file.exists():
                existing = pd.read_parquet(out_file)
                # cast both to same dtypes before concat to avoid type conflicts
                for col in existing.columns:
                    if col in result_df.columns and col not in precomputed:
                        try:
                            result_df[col] = result_df[col].astype(existing[col].dtype)
                        except Exception:
                            existing[col] = existing[col].astype(str)
                            result_df[col] = result_df[col].astype(str)
                result_df = pd.concat([existing, result_df], ignore_index=True)

                # re-aggregate bitmaps for same dim key combinations.
                # group ONLY by real dim cols (never by bitmap/cardinality/hash)
                dim_cols = [c for c in result_df.columns if c not in precomputed]
                has_card = "cardinality" in result_df.columns
                has_hash = "bitmap_hash" in result_df.columns
                group_df = result_df.copy()
                rows = []
                for keys, grp in group_df.groupby(dim_cols, sort=True, dropna= not keep_null):
                    combined = BitMap()
                    for bm_bytes in result_df.loc[grp.index, "bitmap"]:
                        combined |= BitMap.deserialize(bm_bytes)
                    cb_bytes = bytes(combined.serialize())
                    row = dict(zip(dim_cols, keys if isinstance(keys, tuple) else (keys,)))
                    row["bitmap"] = cb_bytes
                    if has_card: row["cardinality"] = len(combined)
                    if has_hash: row["bitmap_hash"] = stable_hash(cb_bytes)
                    rows.append(row)
                result_df = pd.DataFrame(rows)

            pq.write_table(
                pa.Table.from_pandas(result_df),
                out_file,
                compression="zstd",
                row_group_size=row_group_size
            )


# ── Main ──────────────────────────────────────────────────────────────────────

def convolute(entity: str, convolutions_yaml: str, data_dir: str = "./data",
              years: list[int] = None, row_group_size: int = 100_000,
              only: list = None):

    with open(convolutions_yaml) as f:
        conv_specs = yaml.safe_load(f)["convolutions"]

    only_set     = set(only) if only else None
    entity_specs = [c for c in conv_specs
                    if c.get("entity", entity) == entity
                    and c.get("enabled", True)
                    and (only_set is None or c["name"] in only_set)]
    files        = paths_for_entity(data_dir + "/clean", entity, years)
    conv_root    = entity_dir(data_dir + "/convolutions", entity)

    print(f"Convoluting entity={entity}  ({len(files)} files, {len(entity_specs)} specs, row_group_size={row_group_size:,})")

    for f in files:
        with TimerLog("convolute", scope=f.stem) as t:
            convolute_file(f, conv_root, entity_specs, entity, row_group_size=row_group_size)
            t.rows = len(pd.read_parquet(f))
        print(f"  ✓ {f.name}")

    print(f"\nConvolutions → {conv_root}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--entity",          required=True)
    parser.add_argument("--convolutions",    default="config/convolutions.yaml")
    parser.add_argument("--years",           type=str, nargs="+", default=None)
    parser.add_argument("--data-dir",        dest="data_dir", default="./data")
    parser.add_argument("--row-group-size",  dest="row_group_size", type=int, default=100_000,
                        help="Parquet row group size (default: 100000)")
    parser.add_argument("--only",            nargs="+", default=None,
                        help="Only build these convolutions by name, e.g. --only conv_cab_type")
    args = parser.parse_args()
    years = parse_years(args.years) if args.years else None

    convolute(
        entity=args.entity,
        convolutions_yaml=args.convolutions,
        data_dir=args.data_dir,
        years=years,
        row_group_size=args.row_group_size,
        only=args.only,
    )


if __name__ == "__main__":
    main()