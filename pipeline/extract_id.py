"""
pipeline/extract_id.py
----------------------
Adds derived columns (cab_type, year, month) and unique string trip_id
to each parquet file.

Reads from:  data/raw/entities/<entity>/
Writes to:   data/extracted/entities/<entity>/

Usage:
    python -m pipeline.extract_id --entity trips
    python -m pipeline.extract_id --entity trips --years 2009-2015
"""

import argparse
import re
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


def add_col_from_filename(df: pd.DataFrame, col_cfg: dict, filename: str) -> pd.DataFrame:
    col      = col_cfg["col"]
    patterns = col_cfg.get("patterns", {})
    fname    = filename.lower()
    value    = "unknown"
    for label, pattern in patterns.items():
        if re.search(pattern, fname):
            value = label
            break
    df[col] = value
    return df


def add_col_from_filename_regex(df: pd.DataFrame, col_cfg: dict, filename: str) -> pd.DataFrame:
    col   = col_cfg["col"]
    regex = col_cfg["regex"]
    group = col_cfg.get("group", 1)
    m = re.search(regex, filename)
    if not m:
        raise ValueError(f"Cannot extract '{col}' from filename '{filename}' using regex '{regex}'")
    df[col] = m.group(group)
    return df


def apply_add_cols(df: pd.DataFrame, add_cols: list[dict], filename: str) -> pd.DataFrame:
    for col_cfg in add_cols:
        strategy = col_cfg.get("strategy")
        if strategy == "from_filename":
            df = add_col_from_filename(df, col_cfg, filename)
        elif strategy == "from_filename_regex":
            df = add_col_from_filename_regex(df, col_cfg, filename)
    return df


def derive_unique_id(df: pd.DataFrame, unique_id_cfg: dict, filename: str) -> pd.DataFrame:
    col      = unique_id_cfg["col"]
    fmt      = unique_id_cfg["format"]
    df       = df.reset_index(drop=True)
    static   = {}
    for var in re.findall(r"\{(\w+)(?::\w+)?\}", fmt):
        if var == "rownum":
            continue
        if var in df.columns:
            static[var] = str(df[var].iloc[0])
    rownum_fmt = re.search(r"\{rownum(:[\w]+)?\}", fmt)
    if rownum_fmt:
        spec       = rownum_fmt.group(1) or ""
        rownum_strs = df.index.to_series().apply(lambda i: format(i, spec.lstrip(":")))
        base        = re.sub(r"\{rownum(?::\w+)?\}", "|||ROWNUM|||", fmt)
        base        = base.format(**static)
        parts       = base.split("|||ROWNUM|||")
        df[col]     = parts[0] + rownum_strs + (parts[1] if len(parts) > 1 else "")
    else:
        df[col] = fmt.format(**static)
    return df


def extract_id_file(path: Path, out_path: Path, entity_cfg: dict) -> int:
    df        = pd.read_parquet(path)
    add_cols  = entity_cfg.get("add_cols", [])
    unique_id = entity_cfg.get("unique_id", {})
    df        = apply_add_cols(df, add_cols, path.name)
    if unique_id:
        df = derive_unique_id(df, unique_id, path.name)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pandas(df), out_path, compression="zstd")
    return len(df)


def extract_id(entity: str, entities_yaml: str, data_dir: str = "./data",
               years: list[int] = None):
    entity_cfg = load_entity_config(entities_yaml, entity)
    files      = paths_for_entity(data_dir + "/raw", entity, years)
    print(f"Extracting IDs for entity={entity}  ({len(files)} files)")
    for f in files:
        rel      = f.relative_to(entity_dir(data_dir + "/raw", entity))
        out_path = entity_dir(data_dir + "/extracted", entity) / rel
        with TimerLog("extract_id", scope=f.name) as t:
            rows = extract_id_file(f, out_path, entity_cfg)
            t.rows = rows
        print(f"  ✓ {f.name}  {rows:,} rows")
    print(f"\nExtracted → {entity_dir(data_dir + '/extracted', entity)}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--entity",   required=True)
    parser.add_argument("--entities", default="config/entities.yaml")
    parser.add_argument("--years",    type=str, nargs="+", default=None)
    parser.add_argument("--data-dir", dest="data_dir", default="./data")
    args  = parser.parse_args()
    years = parse_years(args.years) if args.years else None
    extract_id(entity=args.entity, entities_yaml=args.entities,
               data_dir=args.data_dir, years=years)


if __name__ == "__main__":
    main()
