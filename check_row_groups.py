"""
check_row_groups.py
-------------------
Prints row group count and row count per convolution file.

Usage:
    python3.11 check_row_groups.py --entity trips
    python3.11 check_row_groups.py --entity trips --conv conv_cab_type
"""

import argparse
from pathlib import Path
import pyarrow.parquet as pq


def check(entity: str, data_dir: str, conv_filter: str = None):
    conv_root  = Path(data_dir) / "convolutions" / "entities" / entity
    facts_root = Path(data_dir) / "facts" / "entities" / entity

    # check convolutions
    if conv_root.exists():
        files = sorted(conv_root.rglob("*.parquet"))
        if conv_filter:
            files = [f for f in files if conv_filter in f.stem]

        print(f"\nCONVOLUTIONS")
        print(f"{'File':<60} {'Rows':>12} {'Row Groups':>12} {'Size MB':>10}")
        print("-" * 98)

        total_files = 0
        single_rg   = 0

        for f in files:
            pf       = pq.ParquetFile(f)
            rows     = pf.metadata.num_rows
            rg_count = pf.metadata.num_row_groups
            size_mb  = f.stat().st_size / 1e6
            rel      = str(f.relative_to(conv_root))

            flag = "⚠" if rg_count == 1 else " "
            print(f"{flag} {rel:<58} {rows:>12,} {rg_count:>12} {size_mb:>9.1f}MB")

            total_files += 1
            if rg_count == 1:
                single_rg += 1

        print("-" * 98)
        print(f"Total files: {total_files}  |  Single row group (⚠): {single_rg}  |  Multi row group: {total_files - single_rg}")

    # check facts
    if facts_root.exists():
        files = sorted(facts_root.rglob("*.parquet"))
        print(f"\nFACTS")
        print(f"{'File':<60} {'Rows':>12} {'Row Groups':>12} {'Size MB':>10}")
        print("-" * 98)

        total_files = total_rows = total_rg = 0
        single_rg = 0

        for f in files:
            pf       = pq.ParquetFile(f)
            rows     = pf.metadata.num_rows
            rg_count = pf.metadata.num_row_groups
            size_mb  = f.stat().st_size / 1e6
            rel      = str(f.relative_to(facts_root))

            flag = "⚠" if rg_count == 1 else " "
            print(f"{flag} {rel:<58} {rows:>12,} {rg_count:>12} {size_mb:>9.1f}MB")

            total_files += 1
            total_rows  += rows
            total_rg    += rg_count
            if rg_count == 1:
                single_rg += 1

        print("-" * 98)
        print(f"Total files: {total_files}  |  Total rows: {total_rows:,}  |  "
              f"Avg row groups/file: {total_rg/total_files:.1f}  |  "
              f"Single row group (⚠): {single_rg}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--entity",   default="trips")
    parser.add_argument("--data-dir", dest="data_dir", default="./data")
    parser.add_argument("--conv",     default=None, help="Filter by convolution name")
    args = parser.parse_args()
    check(args.entity, args.data_dir, args.conv)


if __name__ == "__main__":
    main()