# CAC Benchmark Toolkit v2

Reproducible benchmark pipeline for the Convolution Analytics Cube (CAC).

## Directory Structure

```
data/
├── raw/entities/<entity>/yyyy/         ← downloaded parquet files
├── extracted/entities/<entity>/yyyy/   ← with cab_type, year, month, trip_id added
├── enriched/entities/<entity>/yyyy/    ← with integer entity_id (from Postgres)
├── clean/entities/<entity>/yyyy/       ← valid rows only (normalised schema)
├── dirty/entities/<entity>/yyyy/       ← invalid rows + dirty_reason col
├── facts/entities/<entity>/yyyy/mm/dd/ ← slim fact table for Phase 2
└── convolutions/entities/<entity>/yyyy/mm/dd/  ← bitmap parquets per convolution
```

## Pipeline

```
download     → data/raw/entities/<entity>/
extract_id   → data/extracted/           ← adds cab_type, year, month, trip_id
enrich       → data/enriched/            ← adds integer entity_id via Postgres
clean        → data/clean/ + data/dirty/ ← normalise schema + filter invalid rows
create_facts → data/facts/               ← slim fact table from clean data
convolute    → data/convolutions/        ← bitmaps from clean data
benchmark    → results/
```

## Prerequisites

- Python 3.11+
- Postgres (local or RDS) for integer entity_id assignment
- AWS credentials for `get_vm_cost.py` (read-only `pricing:GetProducts`)

## Setup

```bash
# install dependencies
pip install -e .

# set Postgres DSN
export PG_DSN="postgresql://postgres:postgres@localhost:5433/cac"
```

## Run Pipeline

```bash
# 1. Download NYC taxi data (yellow + green, 2009-2015)
python -m pipeline.download --entity trips --years 2009-2015 --taxi-types yellow green

# 2. Extract IDs (adds cab_type, year, month, trip_id)
python -m pipeline.extract_id --entity trips --years 2009-2015

# 3. Enrich (assign integer entity_id via Postgres)
python -m pipeline.enrich --entity trips --pg-dsn "$PG_DSN" --years 2009-2015

# 4. Clean (normalise schema + filter invalid rows)
python -m pipeline.clean --entity trips --years 2009-2015

# 5. Create facts (slim fact table from clean data)
python -m pipeline.create_facts --entity trips --years 2009-2015

# 6. Build convolutions (bitmap per dimension value from clean data)
python -m pipeline.convolute --entity trips --years 2009-2015
```

## Run Benchmark

```bash
# Run B1 queries against CAC convolutions
python -m benchmark.run --benchmark B1 --entity trips --memory 8GB --duckdb-threads 4

# Consolidate with benchmark source times + VM costs
python -m benchmark.consolidate --benchmark B1 --cac-instance r6i.xlarge
```

## Results

```
results/B1_cac_times.csv       — benchmark_id, query_id, cac_time_ms, speedup
results/B1_consolidated.csv    — with benchmark source times
results/B1_cost.csv            — VM cost comparison
results/logs/pipeline_*.csv    — step-by-step timing
reports/clean_report_*.csv     — per-file clean/dirty counts
reports/enrich_report_*.csv    — per-file enrich timing
```

## Utility Scripts

```bash
# Fix schema on already-enriched data (avoids re-running 4-hour enrich)
python3 fix_schema.py --entity trips

# Verify facts table integrity
python3 verify_facts.py --entity trips
```
