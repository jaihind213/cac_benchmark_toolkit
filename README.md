# CAC Benchmark Toolkit

Reproducible benchmark pipeline for the Convolution Analytics Cube (CAC).

The paper "Convolution Analytics Cube: A New Approach to OLAP" (https://todo) describes the CAC data structure and its performance characteristics. The paper is authored by Mithesh Pathak & Chanderraju Vishnu.

This repository contains a benchmark pipeline that reproduces the results from the paper, using the NYC taxi dataset as a test case.

We have implemented the benchmark queries from the following blog posts:

- [DuckDB 1B Taxi Rides — Mark Litwintschik](https://tech.marksblogg.com/duckdb-1b-taxi-rides.html)
- [ClickHouse and Redshift Face Off Again in NYC Taxi Rides Benchmark — Altinity](https://www.altinity.com/blog/clickhouse-and-redshift-face-off-again-in-nyc-taxi-rides-benchmark)

## Directory Structure

```
data/
├── raw/entities/<entity>/yyyy/            ← downloaded parquet files
├── extracted/entities/<entity>/yyyy/      ← with cab_type, year, month, trip_id added
├── enriched/entities/<entity>/yyyy/       ← with integer entity_id (from Postgres)
├── clean/entities/<entity>/yyyy/          ← valid rows only (normalised schema)
├── dirty/entities/<entity>/yyyy/          ← invalid rows + dirty_reason col
├── facts/entities/<entity>/yyyy/        ← slim fact table for Phase 2
├── facts_duck.db                          ← facts materialised as a native DuckDB table
└── convolutions/entities/<entity>/yyyy/   ← bitmap parquets per convolution
```

## Pipeline

```
download      → data/raw/entities/<entity>/
extract_id    → data/extracted/            ← adds cab_type, year, month, trip_id
enrich        → data/enriched/             ← adds integer entity_id via Postgres
clean         → data/clean/ + data/dirty/  ← normalise schema + filter invalid rows
create_facts  → data/facts/                ← slim fact table from clean data
build_facts_db→ data/facts_duck.db         ← facts as a native DuckDB table (Phase 2 scans)
convolute     → data/convolutions/         ← bitmaps from clean data
benchmark     → results/
```

## Prerequisites

- Python 3.11+
- Postgres (local or RDS) for integer entity_id assignment
- (Optional) AWS credentials for `get_vm_cost.py` — read-only `pricing:GetProducts` — only needed if you want live VM pricing in the cost report

## Setup

```bash
# install dependencies
pip install -e .
# or
cd <project_dir>
micromamba create -n cac_benchmark_toolkit python=3.11 -y
micromamba activate cac_benchmark_toolkit
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

# 5. Create facts (slim fact table), then materialise as a native DuckDB table
python -m pipeline.create_facts --entity trips --row-group-size 500000 --years 2009-2015
python -m pipeline.build_facts_db --entity trips

# 6. Build convolutions (bitmap per dimension value from clean data)
python -m pipeline.convolute --entity trips --years 2009-2015 --row-group-size 100000
```

After building, you can sanity-check that no source file was silently dropped (e.g. a missing month) before benchmarking:

```bash
python verify_convolutions.py --entity trips --cross-check-clean
```

## Run Benchmark

CAC counts distinct entities by summing over roaring bitmaps. Each convolution stores, per dimension value, a bitmap of the entity_ids plus two precomputed columns:

- **`cardinality`** — the bitmap's count, precomputed at build time. Distinct-counting queries can `SUM(cardinality)` natively in DuckDB with no per-row work.
- **`bitmap_hash`** — a stable hash of the bitmap bytes, used as a cache key so bitmaps are deserialised once and reused.

There are two counting modes, each with its own spec:

- **`use_cardinality`** — sums the precomputed `cardinality` column. Fastest; native DuckDB integer sum, no UDF.
- **`use_bitmap_hash`** — calls a UDF that looks up the cached bitmap by its hash and counts it. Useful to measure the cache-backed bitmap path itself.

Most benchmark queries are distinct-counting, so they benefit from the precomputed cardinality. The bitmap-hash mode is provided so you can compare the two approaches directly.

```bash
export NUM_THREADS=16
export MEMORY=24GB

# Cardinality mode (native SUM(cardinality))
python3.11 -m benchmark.run_lookup_bm_hash --benchmark B1_litwintschik_use_cardinality --entity trips --memory $MEMORY --duckdb-threads $NUM_THREADS --iterations 10 --try-to-cache
#sudo sync; echo 3 | sudo tee /proc/sys/vm/drop_caches
python3.11 -m benchmark.run_lookup_bm_hash --benchmark B2_altinity_use_cardinality      --entity trips --memory $MEMORY --duckdb-threads $NUM_THREADS --iterations 10 --try-to-cache
#sudo sync; echo 3 | sudo tee /proc/sys/vm/drop_caches
# Bitmap-hash mode (rb_count_hash UDF over the hash-keyed cache)
python3.11 -m benchmark.run_lookup_bm_hash --benchmark B1_litwintschik_use_bitmap_hash --entity trips --memory $MEMORY --duckdb-threads $NUM_THREADS --iterations 10 --try-to-cache
#sudo sync; echo 3 | sudo tee /proc/sys/vm/drop_caches
python3.11 -m benchmark.run_lookup_bm_hash --benchmark B2_altinity_use_bitmap_hash      --entity trips --memory $MEMORY --duckdb-threads $NUM_THREADS --iterations 10 --try-to-cache
```

## Results

After you run the benchmark, refer to the `results` folder — results are also printed to the console.

We also ran the benchmark ourselves; the compiled results are below.

**Notes:**

- B1-Q2 and B2-Q1 are the same query — a raw data aggregate (`AVG(total_amount)` grouped by `passenger_count`) that is not a distinct count and so does not use CAC's bitmap convolutions. It reads DuckDB's native format of the facts table (see `build_facts_db`) and is included for completeness.
- We categorise the benchmark queries into two kinds: **distinct counting** and **raw data aggregate**. The former benefits from CAC's bitmap convolutions; the latter does not.
- By pairing CAC's bitmap convolutions (for distinct counting) with DuckDB's native columnar storage of the facts table (for raw aggregates), a single system serves both query kinds well.
- **The machine we benchmarked CAC on (AWS m7gd.4xlarge, 16 threads) is smaller and cheaper than the machines in the Litwintschik and Altinity benchmarks (32 threads).** See [Cost Efficiency](#cost-efficiency) below: we compare a roughly half-price, half-core machine against full-size 32-thread machines and still win on speed for distinct-counting queries. On a like-for-like 32-thread machine, the CAC speedups would only be larger.
- All four CAC runs used the same 16-thread machine, so speedup comparisons *between* the CAC modes are like-for-like.
- We chose an instance with local NVMe SSD to avoid EBS charges — cost efficiency is a first-class goal of CAC, alongside query performance.
- Overall: CAC delivers large speedups on distinct-counting queries while remaining competitive on raw aggregates, on cheaper hardware.

Every reported CAC number is validated against the raw SQL over the source data (the `Results match raw SQL` line in each table), so the speedups reflect exact, not approximate, results.

### Dataset size check

```bash
(cac_benchmark_toolkit) [ec2-user@ip-172-31-40-41 cac_benchmark_toolkit]$ export NUM_THREADS=16
export MEMORY=24GB
date; du -sh ./data/*;
Wed Aug 19 01:59:58 UTC 2026
23G	./data/clean
7.8G	./data/convolutions
27M	./data/dirty
21G	./data/enriched
20G	./data/extracted
16G	./data/facts
17G	./data/facts_duck.db
22G	./data/raw
```
The size of the postgres volume we observed, is as follows:

```commandline
(cac_benchmark_toolkit) [ec2-user@ip-172-31-33-66 cac_benchmark_toolkit]$ pwd
/mnt/nvme/cac_benchmark_toolkit
(cac_benchmark_toolkit) [ec2-user@ip-172-31-33-66 cac_benchmark_toolkit]$ date;
Wed Aug 19 01:59:58 UTC 2026
(cac_benchmark_toolkit) [ec2-user@ip-172-31-33-66 cac_benchmark_toolkit]$ cat docker-compose.yaml;
sudo du -sh ../postgres_data
#docker network create test
version: "2"
services:
  postgres:
    image: hbontempo/postgres-hll:15-alpine3.17-latest
    restart: always
    cpus: 2
    environment:
      - POSTGRES_USER=postgres
      - POSTGRES_PASSWORD=postgres
    ports:
      - '5433:5432'
    volumes:
      - /mnt/nvme/postgres_data:/var/lib/postgresql/data
volumes:
  test_db_data:
    driver: local
124G	../postgres_data
(cac_benchmark_toolkit) [ec2-user@ip-172-31-33-66 cac_benchmark_toolkit]$ docker exec -it cac_benchmark_toolkit-postgres-1 psql -U postgres -c "SELECT count(*) FROM cac.trip_ids;"
docker exec -it cac_benchmark_toolkit-postgres-1 psql -U postgres -c "SELECT * FROM cac.trip_ids limit 1;"
   count    
------------
 1206517407
(1 row)

         trip_id         | entity_id  
-------------------------+------------
 2009_01_00000000_yellow | 1206517408
(1 row)

```

To confirm the dataset holds more than one billion trips, we count per cab type directly from the convolution:
Do note: we have some dirty rows in the dataset.

```sql
SELECT cab_type,
       SUM(rb_count_hash(bitmap, bitmap_hash)) AS cnt
FROM conv_cab_type
GROUP BY cab_type;
```

```
+----------+------------+
| cab_type | cnt        |
+----------+------------+
| green    | 35027825   |
| yellow   | 1170834009 |
+----------+------------+
```

(≈1.21 billion trips; validation ✓ MATCH.)

```commandline
setting threads...16
Loading all convolutions (load_all_convolutions=True)
  conv_cab_type: TABLE (555.7MB, 3,286 bitmaps cached by hash) +cardinality
  conv_dropoff_zone: TABLE (1916.3MB, 9,300,063 bitmaps cached by hash) +cardinality
  conv_hour_pickup: TABLE (705.9MB, 61,341 bitmaps cached by hash) +cardinality
  conv_pax_distance_date: TABLE (1850.6MB, 595,730 bitmaps cached by hash) +cardinality
  conv_payment_type: TABLE (688.8MB, 9,699 bitmaps cached by hash) +cardinality
  conv_pickup_zone: TABLE (2311.2MB, 429,500 bitmaps cached by hash) +cardinality
  conv_rate_code: TABLE (320.5MB, 15,457 bitmaps cached by hash) +cardinality
  facts: TABLE via facts_duck.db (17200.9MB, 1,205,861,834 rows, native DuckDB storage)
  taxi_zones: TABLE (265 zones, in-memory, 265 rows inserted)

Memory: 8349.0MB loaded into RAM, 64824MB available
Loaded 8 tables/views
```
### B1 — Litwintschik Benchmark: Cardinality Mode

**Machine:** AWS m7gd.4xlarge (16 threads, 24GB, cache on) · **Results match raw SQL:** 4/4 · CAC (ms) is the median of 10 iterations per query.

| Query | Category | CAC (ms) | CAC threads | Benchmark (ms) | Bench threads | Speedup |
|---|---|---:|---:|---:|---:|---:|
| Q1 | distinct_counting | 0.89 | 16 | 498 | 32 | 559.8x |
| Q3 | distinct_counting | 8.75 | 16 | 734 | 32 | 83.9x |
| Q4 | distinct_counting | 18.12 | 16 | 1334 | 32 | 73.6x |
| Q2 | raw_data_aggregate | 496.72 | 16 | 234 | 32 | 0.5x |

### B2 — Altinity Benchmark: Cardinality Mode

**Machine:** AWS m7gd.4xlarge (16 threads, 24GB, cache on) · **Results match raw SQL:** 5/5 · CAC (ms) is the median of 10 iterations per query.

| Query | Category | CAC (ms) | CAC threads | Benchmark (ms) | Bench threads | Speedup |
|---|---|---:|---:|---:|---:|---:|
| Q2 | distinct_counting | 8.81 | 16 | 1110 | 32 | 126.0x |
| Q3 | distinct_counting | 17.90 | 16 | 1780 | 32 | 99.5x |
| Q4 | distinct_counting | 7.79 | 16 | 940 | 32 | 120.7x |
| Q5 | distinct_counting | 1.59 | 16 | 330 | 32 | 207.6x |
| Q1 | raw_data_aggregate | 496.52 | 16 | 620 | 32 | 1.2x |

### B1 — Litwintschik Benchmark: Bitmap Hash Mode

**Machine:** AWS m7gd.4xlarge (16 threads, 24GB, cache on) · **Results match raw SQL:** 4/4 · CAC (ms) is the median of 10 iterations per query.

| Query | Category | CAC (ms) | CAC threads | Benchmark (ms) | Bench threads | Speedup |
|---|---|---:|---:|---:|---:|---:|
| Q1 | distinct_counting | 44.57 | 16 | 498 | 32 | 11.2x |
| Q3 | distinct_counting | 806.33 | 16 | 734 | 32 | 0.9x |
| Q4 | distinct_counting | 821.72 | 16 | 1334 | 32 | 1.6x |
| Q2 | raw_data_aggregate | 496.53 | 16 | 234 | 32 | 0.5x |
### B2 — Altinity Benchmark: Bitmap Hash Mode

**Machine:** AWS m7gd.4xlarge (16 threads, 24GB, cache on) · **Results match raw SQL:** 5/5 · CAC (ms) is the median of 10 iterations per query.

| Query | Category | CAC (ms) | CAC threads | Benchmark (ms) | Bench threads | Speedup |
|---|---|---:|---:|---:|---:|---:|
| Q2 | distinct_counting | 813.30 | 16 | 1110 | 32 | 1.4x |
| Q3 | distinct_counting | 822.81 | 16 | 1780 | 32 | 2.2x |
| Q4 | distinct_counting | 649.33 | 16 | 940 | 32 | 1.4x |
| Q5 | distinct_counting | 35.09 | 16 | 330 | 32 | 9.4x |
| Q1 | raw_data_aggregate | 496.81 | 16 | 620 | 32 | 1.2x |

Cardinality mode is dramatically faster than bitmap-hash mode on distinct counts, because `SUM(cardinality)` is a native DuckDB integer sum with no per-row UDF, while bitmap-hash mode pays a Python UDF call per bitmap. Both return identical, validated results — the difference is purely in how the count is computed.

To run against cold cache between iterations, drop the OS page cache first:

```bash
sudo sync; echo 3 | sudo tee /proc/sys/vm/drop_caches
```

The screen recording of this run is available in the reference repo: https://github.com/jaihind213/cac_reference_repo/

## Cost Efficiency

Cost efficiency is a first-class goal of CAC, not an afterthought. We deliberately benchmarked CAC on a **smaller, cheaper** machine than the ones used in the source blog posts, and it still comes out ahead on both speed *and* price for the query category CAC targets (distinct counting).

| | CAC benchmark machine | Comparable 32-thread machine |
|---|---|---|
| Instance | `m7gd.4xlarge` (Graviton3, ARM) | `m5.8xlarge` (Intel, x86) |
| vCPUs | 16 | 32 |
| Storage | Local NVMe SSD + 20GB EBS GP3 | 100GB EBS GP3 |
| Monthly cost | **$625.24** | **$1,129.28** |
| 12-month cost | **$7,502.88** | **$13,551.36** |

Prices from the [AWS Pricing Calculator](https://calculator.aws/#/estimate?id=2b9d855af33c03abaf15c5bb99da04947fd3ebcf), US East (Ohio), on-demand, Linux, exported **08/07/2026** (dated screenshot in `aws/estimate_Cost`). AWS prices change over time and by region — re-check before citing.

The comparable full-size machine costs **$504.04/month more (~$6,048/year more)** — about **1.8× the price** — while CAC on the cheaper box is still 10x–500x faster on distinct-counting queries.

Two compounding effects make the comparison favour CAC even more than the headline speedups already show:

- **Half the cores, yet still 10x–500x faster on distinct-counting queries** (see the B1/B2 tables above). The machine that "loses" on core count is the one that wins on speed.
- **Lower price per month, and mostly local NVMe** — the m7gd ships with local NVMe SSD, so it needs only a small EBS volume (20GB here) versus the 100GB the comparison machine carries.

`benchmark.consolidate` folds these VM prices into the results alongside the source blog times, and produces a per-query cost framing: for queries where CAC is faster it reports the annual hardware saving; for queries where CAC is slower it reports how much more per year the faster machine costs.

<a name="pricing-note"></a>
**Pricing note.** AWS on-demand prices change over time and vary by region. The figures above are a point-in-time export (08/07/2026, US East / Ohio). To re-check current rates:

- [AWS EC2 On-Demand Pricing](https://aws.amazon.com/ec2/pricing/on-demand/)
- [Our AWS Pricing Calculator estimate](https://calculator.aws/#/estimate?id=2b9d855af33c03abaf15c5bb99da04947fd3ebcf)
- Or run `python -m pipeline.get_vm_cost --instance m7gd.4xlarge --region <your-region>` (needs read-only `pricing:GetProducts`).

When we ran this benchmark, the `m7gd.4xlarge` cost **$625.24/month** against the `m5.8xlarge`'s **$1,129.28/month** (US East / Ohio, 08/07/2026) — about 55% of the price for the machine that CAC still beats on distinct-counting queries. The m7gd also leans on local NVMe, carrying only a 20GB EBS volume versus 100GB on the comparison machine. A dated screenshot of both estimates is in `aws/estimate_Cost` for a citable record.

## Summary

- **Cost effective.** Our results above are on a machine with half the vCPUs (16 vs 32) and ~55% of the monthly cost ($625 vs $1,129) of the machines used in the source benchmarks — yet still faster on distinct-counting queries.
- **Complementary, not a replacement.** CAC targets distinct counting; for other aggregates it sits alongside ordinary SQL over the raw/facts data in the same system.
- **No silver bullet.** There is no single structure that wins every analytical query — CAC is a sharp tool for a common and expensive class of them.

## Use cases

Distinct counting is a core aggregate across many domains, for example:

- cookie / unique-visitor counting in advertising technology
- counting distinct participants or patients in healthcare analytics
- audience and cohort sizing in experimentation and product analytics

## Source Blogs

This benchmark is based on queries from the following blog posts:

- [DuckDB 1B Taxi Rides — Mark Litwintschik](https://tech.marksblogg.com/duckdb-1b-taxi-rides.html)
- [ClickHouse and Redshift Face Off Again in NYC Taxi Rides Benchmark — Altinity](https://www.altinity.com/blog/clickhouse-and-redshift-face-off-again-in-nyc-taxi-rides-benchmark)

### Disclaimer

We do not own or claim affiliation with the content of these blogs. Their benchmark queries are used here purely as a reference point for our own benchmark.

The pricing figures are point-in-time estimates from the AWS Pricing Calculator (US East / Ohio, on-demand, exported 08/07/2026) and are not affiliated with or endorsed by AWS. AWS on-demand prices change over time and vary by region; the calculator provides an estimate only and excludes taxes and other factors. Verify current rates before relying on these numbers.

### Thanks

We would like to express our sincere gratitude to **Mark Litwintschik** and **Altinity** for publishing their benchmark articles and queries. Their work provided a valuable reference point for our own benchmark and has contributed significantly to the database community's understanding of analytical query performance.

Their work inspired us to create this benchmark, and we are grateful for the foundation they established for the community. We encourage readers to visit the original articles, explore the work in full, and support their continued contributions to the open data and database communities.

### Archived copies

Since these pages may go offline, archived copies are available for reference:

- [Litwintschik Benchmark (archived)](https://web.archive.org/web/20260724041713/https://tech.marksblogg.com/duckdb-1b-taxi-rides.html)
- [Altinity Benchmark (archived)](https://web.archive.org/web/20240530164230/https://altinity.com/blog/clickhouse-and-redshift-face-off-again-in-nyc-taxi-rides-benchmark)

#### PDF snapshots

See the `blogs/snapshots` folder for PDF snapshots of the above blogs.

#### Screen recording

See the reference repo: https://github.com/jaihind213/cac_reference_repo/