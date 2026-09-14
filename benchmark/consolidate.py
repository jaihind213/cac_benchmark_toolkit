"""
benchmark/consolidate.py
------------------------
Consolidate CAC results with the reference benchmark's times and put a COST lens
on every query.

Idea:
  - The reference benchmark ran on an expensive box; CAC ran on a cheaper one.
  - We fetch the ANNUAL on-demand cost of both boxes.
  - Per query we compute the ms difference, then frame it two ways:
      * CAC FASTER  -> "Nx faster AND the box costs $X/yr less"
      * CAC SLOWER  -> "M ms slower; matching that speed costs $X/yr more"
    where $X is the flat box price gap (benchmark_cost - cac_cost).

Inputs:
  1. CAC machine type + region        (--cac-instance, --region)
  2. benchmark id                     (--benchmark)
  3. reference hardware               (from the benchmark yaml: cloud_equivalent)
  4. CAC results                      (results/{id}_cac_times*.csv)

Writes:
  - results/{benchmark_id}_consolidated.csv   (per-query, with cost framing)
  - results/{benchmark_id}_cost.csv           (the two-box cost summary)

Usage:
    python -m benchmark.consolidate --benchmark B1_litwintschik_use_cardinality \
        --cac-instance m7gd.4xlarge --region ap-southeast-1
    # a prefix expands to every matching spec that has results:
    python -m benchmark.consolidate --benchmark B1 --cac-instance m7gd.4xlarge
"""

import argparse
import csv
from datetime import date
from pathlib import Path

import pandas as pd
import yaml

from pipeline.get_vm_cost import get_ec2_price

RESULTS_DIR = Path("results")
MONTHS_PER_YEAR = 12


def _annual(monthly: float) -> float:
    return monthly * MONTHS_PER_YEAR


def _price_annual(instance: str, region: str):
    """Return (monthly_usd, annual_usd) or (None, None) on failure."""
    try:
        p = get_ec2_price(instance, region)
        m = p["price_per_month_usd"]
        return m, _annual(m)
    except Exception as e:
        print(f"  could not fetch price for {instance} in {region}: {e}")
        return None, None


def consolidate(benchmark_id: str, cac_instance: str, region: str):
    # ── load spec (exact id match preferred) ──
    candidates = sorted(Path("config/benchmarks").glob(f"{benchmark_id}*.yaml"))
    exact = [c for c in candidates if c.stem == benchmark_id]
    if exact:
        candidates = exact
    if not candidates:
        raise FileNotFoundError(f"No spec found for {benchmark_id}")
    with open(candidates[0]) as f:
        spec = yaml.safe_load(f)

    # ── load CAC results (filename carries threads/memory/cache suffixes) ──
    result_files = sorted(
        RESULTS_DIR.glob(f"{benchmark_id}_cac_times*.csv"),
        key=lambda p: p.stat().st_mtime,
    )
    if not result_files:
        raise FileNotFoundError(
            f"CAC results not found for '{benchmark_id}'. Looked for "
            f"{RESULTS_DIR}/{benchmark_id}_cac_times*.csv — run run_benchmark.py first."
        )
    cac_file = result_files[-1]
    if len(result_files) > 1:
        print(f"  (using most recent of {len(result_files)} result files: {cac_file.name})")
    cac_df = pd.read_csv(cac_file)

    # ── fetch both box costs (annual on-demand) ──
    hw                 = spec.get("hardware", {})
    benchmark_instance = hw.get("cloud_equivalent")
    benchmark_region   = hw.get("region", region)

    bm_month,  bm_year  = _price_annual(benchmark_instance, benchmark_region) \
                          if benchmark_instance else (None, None)
    cac_month, cac_year = _price_annual(cac_instance, region)

    # flat annual box-price gap — the same for every query
    year_gap = (bm_year - cac_year) if (bm_year is not None and cac_year is not None) else None

    print(f"\n{'='*84}")
    print(f"Consolidating: {benchmark_id} — {spec['name']}")
    print(f"{'='*84}")
    if bm_year is not None:
        print(f"  Benchmark box: {benchmark_instance} ({benchmark_region})   "
              f"${bm_month:,.0f}/mo   ${bm_year:,.0f}/yr")
    if cac_year is not None:
        print(f"  CAC box:       {cac_instance} ({region})   "
              f"${cac_month:,.0f}/mo   ${cac_year:,.0f}/yr")
    if year_gap is not None:
        cheaper = "cheaper" if year_gap >= 0 else "more expensive"
        print(f"  Box price gap: ${abs(year_gap):,.0f}/yr  (CAC is {cheaper})")

    # ── per-query rows with cost framing ──
    rows = []
    n_faster = n_slower = 0
    for q in spec["queries"]:
        if not q.get("enabled", True):
            continue
        qid      = q["id"]
        cac_row  = cac_df[cac_df["query_id"] == qid]
        if cac_row.empty:
            continue
        cac_time = float(cac_row["cac_time_ms"].values[0])
        bm_time  = q.get("benchmark_time_ms")
        if bm_time is None:
            continue

        delta_ms = cac_time - bm_time              # negative => CAC faster
        faster   = delta_ms < 0
        speedup  = bm_time / cac_time if cac_time else None

        if faster:
            n_faster += 1
            verdict  = "CAC_FASTER"
            if year_gap is not None:
                framing = (f"{speedup:.1f}x faster; saves ${year_gap:,.0f}/yr on hardware"
                           if year_gap >= 0 else
                           f"{speedup:.1f}x faster; box costs ${-year_gap:,.0f}/yr more")
            else:
                framing = f"{speedup:.1f}x faster"
        else:
            n_slower += 1
            verdict  = "CAC_SLOWER"
            if year_gap is not None and year_gap >= 0:
                framing = (f"{delta_ms:,.0f} ms slower; matching it costs "
                           f"${year_gap:,.0f}/yr more hardware")
            elif year_gap is not None:
                framing = (f"{delta_ms:,.0f} ms slower AND box costs "
                           f"${-year_gap:,.0f}/yr more")
            else:
                framing = f"{delta_ms:,.0f} ms slower"

        rows.append({
            "benchmark_id":       benchmark_id,
            "query_id":           qid,
            "category":           q.get("category", ""),
            "nature":             q["nature"],
            "cac_phase":          q.get("cac_phase", ""),
            "benchmark_time_ms":  bm_time,
            "cac_time_ms":        round(cac_time, 3),
            "delta_ms":           round(delta_ms, 1),
            "speedup":            f"{speedup:.1f}x" if speedup else "N/A",
            "verdict":            verdict,
            "box_price_gap_year_usd": round(year_gap, 2) if year_gap is not None else "",
            "cost_framing":       framing,
            "match_status":       cac_row["match_status"].values[0] if "match_status" in cac_row else "",
        })

    consolidated = pd.DataFrame(rows)
    out_file = RESULTS_DIR / f"{benchmark_id}_consolidated.csv"
    consolidated.to_csv(out_file, index=False)

    # ── print: faster first, slower last ──
    print(f"\n── Per-query verdict ({n_faster} faster, {n_slower} slower) ──")
    order = consolidated.copy()
    order["_o"] = (order["verdict"] == "CAC_SLOWER").astype(int)
    order = order.sort_values(["_o", "query_id"])
    for _, r in order.iterrows():
        tag = "\u2713 faster" if r["verdict"] == "CAC_FASTER" else "\u2717 slower"
        print(f"  {r['query_id']:<4} {tag}  cac={r['cac_time_ms']:>10.2f}ms  "
              f"bench={r['benchmark_time_ms']:>6}ms  | {r['cost_framing']}")
    print(f"\n\u2713 Written \u2192 {out_file}")

    # ── cost summary file ──
    cost_row = {
        "benchmark_id":                benchmark_id,
        "benchmark_vm":                benchmark_instance or hw.get("actual", ""),
        "benchmark_vm_cost_month_usd": round(bm_month, 2) if bm_month is not None else "",
        "benchmark_vm_cost_year_usd":  round(bm_year, 2)  if bm_year  is not None else "",
        "benchmark_cores_used":        hw.get("cores_used", ""),
        "benchmark_cost_ref_url":      spec.get("url", ""),
        "cac_vm":                      cac_instance,
        "cac_vm_cost_month_usd":       round(cac_month, 2) if cac_month is not None else "",
        "cac_vm_cost_year_usd":        round(cac_year, 2)  if cac_year  is not None else "",
        "box_price_gap_year_usd":      round(year_gap, 2)  if year_gap  is not None else "",
        "queries_faster":              n_faster,
        "queries_slower":              n_slower,
        "benchmark_hardware_note":     hw.get("note", ""),
        "prices_as_of":                date.today().isoformat(),
        "cac_region":                  region,
        "benchmark_region":            benchmark_region,
    }
    cost_file = RESULTS_DIR / f"{benchmark_id}_cost.csv"
    with open(cost_file, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cost_row.keys())
        w.writeheader()
        w.writerow(cost_row)
    print(f"\u2713 Cost written \u2192 {cost_file}")


def resolve_benchmark_ids(requested: str) -> list:
    specs = sorted(Path("config/benchmarks").glob("*.yaml"))
    stems = [p.stem for p in specs]
    if requested in stems:
        return [requested]
    return [s for s in stems if s.startswith(requested)]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--benchmark",    nargs="+", required=True)
    parser.add_argument("--cac-instance", dest="cac_instance", required=True,
                        help="EC2 instance type used for CAC, e.g. m7gd.4xlarge")
    parser.add_argument("--region",       default="us-east-1")
    args = parser.parse_args()

    resolved = []
    for token in args.benchmark:
        ids = resolve_benchmark_ids(token)
        if not ids:
            print(f"  warning: no spec matches '{token}' — skipping")
        resolved.extend(ids)
    if not resolved:
        raise SystemExit("No matching benchmark specs found.")

    for bm_id in resolved:
        try:
            consolidate(bm_id, args.cac_instance, args.region)
        except FileNotFoundError as e:
            print(f"  skip {bm_id}: {e}")


if __name__ == "__main__":
    main()