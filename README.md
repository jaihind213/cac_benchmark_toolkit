# CAC Benchmark Toolkit

Reproducible benchmark pipeline for the Convolution Analytics Cube (CAC).

The paper "Convolution Analytics Cube: A New Approach to OLAP" (https://todo) describes the CAC data structure and its performance characteristics. 

The paper is authored by Mithesh Pathak & Chanderraju Vishnu.

This repository contains a benchmark pipeline that reproduces the results from the paper, using the NYC taxi dataset as a test case.

We have implemented the benchmark queries from the following blog posts:

- [DuckDB 1B Taxi Rides — Mark Litwintschik](https://tech.marksblogg.com/duckdb-1b-taxi-rides.html)
- [ClickHouse and Redshift Face Off Again in NYC Taxi Rides Benchmark — Altinity ](https://www.altinity.com/blog/clickhouse-and-redshift-face-off-again-in-nyc-taxi-rides-benchmark)


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
# or
cd <project_dir>
micromamba create -n cac_benchmark_toolkit python=3.11 -y
micromamba install poetry
poetry lock
poetry install --no-root

# set Postgres DSN
docker-compose up -d
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
python -m pipeline.build_facts_db --entity trips

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

After you run the benchmark, refer to the `results` folder — results are also printed to the console.

**Notes:**
- B1-Q2 and B2-Q1 are the same query, a raw data aggregate query not optimized for CAC. It's included for completeness. This query reads DuckDB's native format of the facts table.
- We have categorized the benchmark queries into two categories: distinct counting and raw data aggregate. The former benefits from CAC's bitmap convolutions, while the latter does not.
- However by using CAC's bitmap convolutions along with Duckdb's native format of the facts table, we can achieve better performance for raw data aggregate queries as well and still maintain the benefits of CAC's bitmap convolutions for distinct counting queries.
- The benchmark machine we used (AWS m7gd.4xlarge, 16 threads) differs from the Mark Litwintschik and Altinity benchmark machines (32 threads).
- The CAC benchmark we ran, was on a 16-thread machine, so speedup comparisons *between* the different CAC benchmark runs are valid.
- A machine with local SSD NVMe storage was chosen to avoid EBS costs — cost efficiency is one of CAC's key goals, alongside better query performance.
- As the benchmark demonstrates, CAC can achieve significant speedups for distinct counting queries, while still maintaining competitive performance for raw data aggregate queries, while being cost efficient.

---

To be sure we counted more than 1 billion trips, we ran the following query:

```sql
SELECT cab_type,
             SUM(rb_count_hash(bitmap, bitmap_hash)) AS cnt
      FROM conv_cab_type
      GROUP BY cab_type
  Q1     Distinct count per dimension value (cab type)         44.57ms           498ms  speedup=11.2x
         validation: ✓ MATCH
         +----------+--------------+
         | cab_type | count_star() |
         +----------+--------------+
         | green    | 35027825     |
         | yellow   | 1170834009   |
         +----------+--------------+
```

### B1 — Litwintschik Benchmark: Cardinality Mode

**Machine:** AWS m7gd.4xlarge (16 threads, 24GB, cache on)
**Results match raw SQL:** 4/4
**CAC (ms)** is the median of 10 iterations per query.

| Query | Category            | CAC (ms) | CAC threads | Benchmark (ms) | Bench threads | Speedup |
|-------|----------------------|---------:|:-----------:|----------------:|:--------------:|--------:|
| Q1    | distinct_counting    |     0.89 |      16     |             498 |       32       | 559.6x  |
| Q3    | distinct_counting    |     8.81 |      16     |             734 |       32       |  83.3x  |
| Q4    | distinct_counting    |    17.88 |      16     |            1334 |       32       |  74.6x  |
| Q2    | raw_data_aggregate   |   496.31 |      16     |             234 |       32       |   0.5x  |

```bash
sudo sync; echo 3 | sudo tee /proc/sys/vm/drop_caches
```

---

### B2 — Altinity Benchmark: Cardinality Mode

**Machine:** AWS m7gd.4xlarge (16 threads, 24GB, cache on)
**Results match raw SQL:** 5/5
**CAC (ms)** is the median of 10 iterations per query.

| Query | Category            | CAC (ms) | CAC threads | Benchmark (ms) | Bench threads | Speedup |
|-------|----------------------|---------:|:-----------:|----------------:|:--------------:|--------:|
| Q2    | distinct_counting    |     8.79 |      16     |            1110 |       32       | 126.3x  |
| Q3    | distinct_counting    |    18.37 |      16     |            1780 |       32       |  96.9x  |
| Q4    | distinct_counting    |     7.76 |      16     |             940 |       32       | 121.1x  |
| Q5    | distinct_counting    |     1.58 |      16     |             330 |       32       | 208.8x  |
| Q1    | raw_data_aggregate   |   496.56 |      16     |             620 |       32       |   1.2x  |

```bash
sudo sync; echo 3 | sudo tee /proc/sys/vm/drop_caches
```

---

### B1 — Litwintschik Benchmark: Bitmap Hash Mode

**Machine:** AWS m7gd.4xlarge (16 threads, 24GB, cache on)
**Results match raw SQL:** 4/4
**CAC (ms)** is the median of 10 iterations per query.

| Query | Category            | CAC (ms) | CAC threads | Benchmark (ms) | Bench threads | Speedup |
|-------|----------------------|---------:|:-----------:|----------------:|:--------------:|--------:|
| Q1    | distinct_counting    |    44.59 |      16     |             498 |       32       |  11.2x  |
| Q3    | distinct_counting    |   703.76 |      16     |             734 |       32       |   1.0x  |
| Q4    | distinct_counting    |   720.96 |      16     |            1334 |       32       |   1.9x  |
| Q2    | raw_data_aggregate   |   496.10 |      16     |             234 |       32       |   0.5x  |

```bash
sudo sync; echo 3 | sudo tee /proc/sys/vm/drop_caches
```

---

### B2 — Altinity Benchmark: Bitmap Hash Mode

**Machine:** AWS m7gd.4xlarge (16 threads, 24GB, cache on)
**Results match raw SQL:** 5/5
**CAC (ms)** is the median of 10 iterations per query.

| Query | Category            | CAC (ms) | CAC threads | Benchmark (ms) | Bench threads | Speedup |
|-------|----------------------|---------:|:-----------:|----------------:|:--------------:|--------:|
| Q2    | distinct_counting    |   696.99 |      16     |            1110 |       32       |   1.6x  |
| Q3    | distinct_counting    |   712.60 |      16     |            1780 |       32       |   2.5x  |
| Q4    | distinct_counting    |   610.16 |      16     |             940 |       32       |   1.5x  |
| Q5    | distinct_counting    |    27.02 |      16     |             330 |       32       |  12.2x  |
| Q1    | raw_data_aggregate   |   496.90 |      16     |             620 |       32       |   1.2x  |

```bash
sudo sync; echo 3 | sudo tee /proc/sys/vm/drop_caches
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