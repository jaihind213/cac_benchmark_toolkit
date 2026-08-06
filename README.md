# CAC Benchmark Toolkit v2

Reproducible benchmark pipeline for the Convolution Analytics Cube (CAC).

The paper "Convolution Analytics Cube: A New Approach to OLAP" (https://todo) describes the CAC data structure and its performance characteristics. 

This repository contains a benchmark pipeline that reproduces the results from the paper, using the NYC taxi dataset as a test case.

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
export PG_DSN="postgresql://postgres:postgres@localhost:5433/postgres"
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

# 5. Create facts (slim fact table from clean data).
python -m pipeline.create_facts --entity trips --row-group-size 500000 --years 2009-2015

# 6. Build convolutions (bitmap per dimension value from clean data)
python -m pipeline.convolute --entity trips --years 2009-2015 --row-group-size 100000
```

## Run Benchmark

```bash
# Run benchmarks queries against CAC convolutions.
# There are variants of the benchmark queries, so you can run them all and compare results.
export NUM_THREADS=8
export MEMORY=8GB
# you have 2 options for the benchmark: use bitmap hash or cardinality
# we pre compute bitmap cardinality in the convolutions, so you can use that for faster queries.
# we also cache the bitmap hashes in memory, so you can use that for faster queries. this saves on the fly computation of bitmap hashes & deserialization of bitmaps from parquet.
# Since most of the Benchmark queries are around distinct counting, they can take advantage of the pre-computed bitmap cardinality. However, 
# if you want to test the performance of the bitmap hash approach, you can use that as well.
python3.11 -m benchmark.run_lookup_bm_hash --benchmark B1_litwintschik_use_cardinality --entity trips --memory $MEMORY --duckdb-threads $NUM_THREADS --iterations 10 --try-to-cache 
python3.11 -m benchmark.run_lookup_bm_hash --benchmark B2_altinity_use_cardinality --entity trips --memory $MEMORY --duckdb-threads $NUM_THREADS --iterations 10 --try-to-cache 
#
python3.11 -m benchmark.run_lookup_bm_hash --benchmark B1_litwintschik_use_bitmap_hash --entity trips --memory $MEMORY --duckdb-threads $NUM_THREADS --iterations 10 --try-to-cache
python3.11 -m benchmark.run_lookup_bm_hash --benchmark B2_altinity_use_bitmap_hash --entity trips --memory $MEMORY --duckdb-threads $NUM_THREADS --iterations 10 --try-to-cache
 

# Consolidate with benchmark source times + VM costs
python -m benchmark.consolidate --benchmark B1 --cac-instance r6i.xlarge
```

## Results

```
please refer to 'results' folder.
```

## Source Blogs

This benchmark is based on queries from the following blog posts:

- [DuckDB 1B Taxi Rides — Mark Litwintschik](https://tech.marksblogg.com/duckdb-1b-taxi-rides.html)
- [ClickHouse and Redshift Face Off Again in NYC Taxi Rides Benchmark — Altinity](https://www.altinity.com/blog/clickhouse-and-redshift-face-off-again-in-nyc-taxi-rides-benchmark)

### Disclaimer

We do not own or claim affiliation with the content of these blogs. Their benchmark queries are used here purely as a reference point for our own benchmark.

### Thanks

We would like to express our sincere gratitude to **Mark Litwintschik** and **Altinity** for publishing their benchmark articles and queries. Their work provided a valuable reference point for our own benchmark and has contributed significantly to the database community's understanding of analytical query performance.

Their work inspired us to create this benchmark, and we are grateful for the foundation they established for the community.

We encourage readers to visit their original articles, explore their work in full, and support their continued contributions to the open data and database communities.

### Archived copies

Since these pages may go offline, their archived copies are available for reference:

- [Litwintschik Benchmark (archived)](https://web.archive.org/web/20260724041713/https://tech.marksblogg.com/duckdb-1b-taxi-rides.html)
- [Altinity Benchmark (archived)](https://web.archive.org/web/20240530164230/https://altinity.com/blog/clickhouse-and-redshift-face-off-again-in-nyc-taxi-rides-benchmark)

#### PDF snapshots of the above blogs

refer to the `blogs/snapshots` folder for pdf snapshots of the above blogs.

#### Screen recording of the above blogs

todo: