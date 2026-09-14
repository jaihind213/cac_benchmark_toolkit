"""
verify_convolutions.py
----------------------
Post-build integrity check for convolutions. Catches silently-dropped source
files (like a missing month) BEFORE they surface as benchmark mismatches.

For each convolution it reports:
  - distinct pickup_date count and the full date range
  - any missing days within that range (grouped by month)
  - total SUM(cardinality) — the trip count the convolution represents
  - optional cross-check of SUM(cardinality) vs the clean row count

A convolution built from complete data should have a contiguous run of dates
with no interior gaps. A whole month missing (e.g. June 2011) means a source
file was dropped during the build and the convolution must be rebuilt.

Usage:
    python verify_convolutions.py --entity trips
    python verify_convolutions.py --entity trips --expect-start 2009-01-01 --expect-end 2015-12-31
    python verify_convolutions.py --entity trips --cross-check-clean
    python verify_convolutions.py --entity trips --only conv_pax_distance_date conv_pickup_zone
"""

import argparse
import datetime
from collections import Counter
from pathlib import Path

import duckdb


def all_days(start: datetime.date, end: datetime.date) -> set:
    days, d = set(), start
    while d <= end:
        days.add(str(d))
        d += datetime.timedelta(days=1)
    return days


def parse_date(s: str) -> datetime.date:
    return datetime.datetime.strptime(s, "%Y-%m-%d").date()


def discover_convolutions(conv_root: Path) -> list[str]:
    names = set()
    for f in conv_root.rglob("*.parquet"):
        names.add(f.stem)
    return sorted(names)


def verify_one(con, conv_root: Path, name: str,
               expect_start: str = None, expect_end: str = None) -> dict:
    glob = str(conv_root / "**" / f"{name}.parquet")
    con.execute(
        f"CREATE OR REPLACE VIEW v AS "
        f"SELECT * FROM read_parquet('{glob}', hive_partitioning=false)"
    )

    cols = {r[1] for r in con.execute("PRAGMA table_info('v')").fetchall()}
    has_pickup_date = "pickup_date" in cols
    has_card        = "cardinality" in cols

    result = {"name": name, "has_pickup_date": has_pickup_date,
              "has_cardinality": has_card}

    if not has_pickup_date:
        result["note"] = "no pickup_date column — cannot check date coverage"
        return result

    row = con.execute(
        "SELECT MIN(pickup_date), MAX(pickup_date), COUNT(DISTINCT pickup_date) FROM v"
    ).fetchone()
    dmin, dmax, ndistinct = row
    result.update({"date_min": str(dmin), "date_max": str(dmax),
                   "distinct_dates": ndistinct})

    if has_card:
        result["total_cardinality"] = con.execute(
            "SELECT SUM(cardinality) FROM v"
        ).fetchone()[0]

    # determine the expected date window
    start = parse_date(expect_start) if expect_start else parse_date(str(dmin))
    end   = parse_date(expect_end)   if expect_end   else parse_date(str(dmax))

    present = {r[0] for r in con.execute(
        "SELECT DISTINCT pickup_date FROM v").fetchall()}
    present = {str(p) for p in present}

    expected = all_days(start, end)
    missing  = sorted(expected - present)
    result["missing_count"] = len(missing)
    if missing:
        bymonth = Counter(m[:7] for m in missing)
        # flag months that are ENTIRELY missing (>= 28 days) — a dropped file
        whole_months = {ym: c for ym, c in bymonth.items() if c >= 28}
        result["missing_by_month"]   = dict(sorted(bymonth.items()))
        result["whole_months_gone"]  = dict(sorted(whole_months.items()))
        result["missing_sample"]     = missing[:5]

    return result


def cross_check_clean(con, conv_root: Path, data_dir: str, entity: str,
                      name: str) -> str:
    """Compare SUM(cardinality) to the clean row count (only meaningful for
    convolutions whose bitmaps partition all trips exactly once, e.g. by date)."""
    clean_glob = str(Path(data_dir) / "clean" / "entities" / entity / "**" / "*.parquet")
    conv_glob  = str(conv_root / "**" / f"{name}.parquet")
    try:
        clean_n = con.execute(
            f"SELECT COUNT(*) FROM read_parquet('{clean_glob}', "
            f"hive_partitioning=false, union_by_name=true)"
        ).fetchone()[0]
        conv_n = con.execute(
            f"SELECT SUM(cardinality) FROM read_parquet('{conv_glob}', "
            f"hive_partitioning=false)"
        ).fetchone()[0]
        conv_n = int(conv_n) if conv_n is not None else 0
        delta  = clean_n - conv_n
        flag   = "✓" if delta == 0 else f"✗ delta={delta:,}"
        return f"clean={clean_n:,}  conv={conv_n:,}  {flag}"
    except Exception as e:
        return f"(cross-check failed: {e})"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--entity", required=True)
    ap.add_argument("--data-dir", dest="data_dir", default="./data")
    ap.add_argument("--only", nargs="+", default=None,
                    help="Only check these convolutions by name")
    ap.add_argument("--expect-start", default=None,
                    help="Expected first date YYYY-MM-DD (default: observed min)")
    ap.add_argument("--expect-end", default=None,
                    help="Expected last date YYYY-MM-DD (default: observed max)")
    ap.add_argument("--cross-check-clean", action="store_true",
                    help="Also compare SUM(cardinality) to clean row count")
    args = ap.parse_args()

    conv_root = Path(args.data_dir) / "convolutions" / "entities" / args.entity
    if not conv_root.exists():
        print(f"Not found: {conv_root}")
        return

    names = discover_convolutions(conv_root)
    if args.only:
        names = [n for n in names if n in set(args.only)]

    con = duckdb.connect()
    print(f"Verifying {len(names)} convolutions for entity={args.entity}")
    if args.expect_start or args.expect_end:
        print(f"Expected window: {args.expect_start or '(min)'} .. {args.expect_end or '(max)'}")
    print("=" * 78)

    problems = []
    for name in names:
        r = verify_one(con, conv_root, name, args.expect_start, args.expect_end)

        if not r.get("has_pickup_date"):
            print(f"\n{name}: {r.get('note','no date column')}")
            continue

        card = f"  cardinality={r['total_cardinality']:,}" if r.get("total_cardinality") is not None else ""
        print(f"\n{name}")
        print(f"  dates: {r['date_min']} .. {r['date_max']}  "
              f"({r['distinct_dates']} distinct){card}")

        if r["missing_count"] == 0:
            print(f"  ✓ no missing days in range")
        else:
            print(f"  ✗ {r['missing_count']} missing days")
            whole = r.get("whole_months_gone") or {}
            if whole:
                for ym, c in whole.items():
                    print(f"      ⚠ ENTIRE MONTH missing: {ym} ({c} days) "
                          f"— likely a dropped source file, REBUILD")
                problems.append((name, whole))
            # show non-whole-month gaps briefly
            partial = {ym: c for ym, c in r["missing_by_month"].items()
                       if ym not in whole}
            if partial:
                shown = ", ".join(f"{ym}:{c}d" for ym, c in list(partial.items())[:6])
                print(f"      partial gaps: {shown}"
                      f"{' ...' if len(partial) > 6 else ''}")

        if args.cross_check_clean and r.get("has_cardinality"):
            print(f"  clean cross-check: "
                  f"{cross_check_clean(con, conv_root, args.data_dir, args.entity, name)}")

    print("\n" + "=" * 78)
    if problems:
        print(f"✗ {len(problems)} convolution(s) have whole months missing — rebuild these:")
        for name, whole in problems:
            months = ",".join(whole.keys())
            print(f"    {name}  (missing months: {months})")
        print("\nRebuild example:")
        bad = problems[0][0]
        yrs = sorted({ym[:4] for _, whole in problems for ym in whole})
        print(f"    find data/convolutions/entities/{args.entity}/{{{','.join(yrs)}}} "
              f"-name '{bad}.parquet' -delete")
        print(f"    python -m pipeline.convolute --entity {args.entity} "
              f"--years {' '.join(yrs)} --only {bad}")
    else:
        print("✓ All checked convolutions have contiguous date coverage — no dropped files.")


if __name__ == "__main__":
    main()
