"""
benchmark/consolidate.py
------------------------
Consolidates CAC results with benchmark source times and VM costs.

Reads:
  - results/{benchmark_id}_cac_times.csv    (from benchmark/run.py)
  - config/benchmarks/{benchmark_id}_*.yaml (benchmark_time_ms + vm spec)

Fetches:
  - Benchmark VM cost via pipeline/get_vm_cost.py
  - CAC VM cost via pipeline/get_vm_cost.py (from --cac-instance)

Writes:
  - results/{benchmark_id}_consolidated.csv
  - results/{benchmark_id}_cost.csv

Usage:
    python -m benchmark.consolidate --benchmark B1 --cac-instance r6i.xlarge
    python -m benchmark.consolidate --benchmark B1 B2 --cac-instance r6i.xlarge --region us-east-1
"""

import argparse
import csv
from datetime import date
from pathlib import Path

import pandas as pd
import yaml

from pipeline.get_vm_cost import get_ec2_price

RESULTS_DIR = Path("results")


def consolidate(benchmark_id: str, cac_instance: str, region: str):
    # ── load spec ──
    candidates = list(Path("config/benchmarks").glob(f"{benchmark_id}*.yaml"))
    if not candidates:
        raise FileNotFoundError(f"No spec found for {benchmark_id}")
    with open(candidates[0]) as f:
        spec = yaml.safe_load(f)

    # ── load CAC results ──
    cac_file = RESULTS_DIR / f"{benchmark_id}_cac_times.csv"
    if not cac_file.exists():
        raise FileNotFoundError(f"CAC results not found: {cac_file}. Run benchmark/run.py first.")

    cac_df = pd.read_csv(cac_file)

    # ── build query comparison ──
    print(f"\n{'='*70}")
    print(f"Consolidating: {benchmark_id} — {spec['name']}")
    print(f"{'='*70}")

    rows = []
    for q in spec["queries"]:
        qid       = q["id"]
        cac_row   = cac_df[cac_df["query_id"] == qid]
        cac_time  = float(cac_row["cac_time_ms"].values[0]) if not cac_row.empty else None
        bm_time   = q.get("benchmark_time_ms")
        speedup   = f"{bm_time/cac_time:.1f}x" if (bm_time and cac_time) else "N/A"

        rows.append({
            "benchmark_id":       benchmark_id,
            "query_id":           qid,
            "nature":             q["nature"],
            "cac_phase":          q.get("cac_phase", ""),
            "benchmark_time_ms":  bm_time,
            "cac_time_ms":        round(cac_time, 3) if cac_time else "",
            "speedup":            speedup,
            "dimension_note":     q.get("dimension_note", ""),
        })

    consolidated = pd.DataFrame(rows)

    out_file = RESULTS_DIR / f"{benchmark_id}_consolidated.csv"
    consolidated.to_csv(out_file, index=False)
    print(f"\n── Query Comparison ──")
    print(consolidated[["query_id","nature","benchmark_time_ms","cac_time_ms","speedup","cac_phase"]].to_string(index=False))
    print(f"\n✓ Written → {out_file}")

    # ── VM cost comparison ──
    print(f"\n── VM Cost Comparison ──")
    hw = spec.get("hardware", {})
    benchmark_instance = hw.get("cloud_equivalent")
    benchmark_region   = hw.get("region", region)

    benchmark_cost = None
    cac_cost       = None

    if benchmark_instance:
        try:
            benchmark_cost = get_ec2_price(benchmark_instance, benchmark_region)
            print(f"  Benchmark VM ({benchmark_instance}): ${benchmark_cost['price_per_month_usd']:.2f}/month")
        except Exception as e:
            print(f"  Could not fetch benchmark VM cost: {e}")

    try:
        cac_cost = get_ec2_price(cac_instance, region)
        print(f"  CAC VM ({cac_instance}):       ${cac_cost['price_per_month_usd']:.2f}/month")
    except Exception as e:
        print(f"  Could not fetch CAC VM cost: {e}")

    cost_ratio = None
    if benchmark_cost and cac_cost:
        cost_ratio = benchmark_cost["price_per_month_usd"] / cac_cost["price_per_month_usd"]
        print(f"  Cost ratio (p/k):              {cost_ratio:.1f}x cheaper with CAC")

    cost_row = {
        "benchmark_id":               benchmark_id,
        "benchmark_vm":               benchmark_instance or hw.get("actual", ""),
        "benchmark_vm_cost_month_usd": benchmark_cost["price_per_month_usd"] if benchmark_cost else "",
        "benchmark_cost_ref_url":     spec.get("url", ""),
        "cac_vm":                     cac_instance,
        "cac_vm_cost_month_usd":      cac_cost["price_per_month_usd"] if cac_cost else "",
        "cost_ratio":                 f"{cost_ratio:.1f}x" if cost_ratio else "",
        "benchmark_hardware_note":    hw.get("note", ""),
        "prices_as_of":               date.today().isoformat(),
        "region":                     region,
    }

    cost_file = RESULTS_DIR / f"{benchmark_id}_cost.csv"
    with open(cost_file, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cost_row.keys())
        w.writeheader()
        w.writerow(cost_row)
    print(f"✓ Cost written → {cost_file}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--benchmark",    nargs="+", required=True)
    parser.add_argument("--cac-instance", dest="cac_instance", required=True,
                        help="EC2 instance type used for CAC e.g. r6i.xlarge")
    parser.add_argument("--region",       default="us-east-1")
    args = parser.parse_args()

    for bm_id in args.benchmark:
        consolidate(bm_id, args.cac_instance, args.region)


if __name__ == "__main__":
    main()
