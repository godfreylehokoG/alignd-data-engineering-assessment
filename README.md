# Alignd Data Engineering Assessment: Resilient Pipelines

## 👤 Candidate
**Lehlohonolo [Your Last Name]**

---

## Overview

End-to-end data pipeline for processing healthcare data — from raw file ingestion through cloud ETL, data cleaning, warehouse modelling, quality checks, and automated transformations.

The pipeline processes three healthcare datasets (clients, products, policy lapses), loads them into a PostgreSQL star schema, applies data quality transformations, and produces analytics-ready models via dbt.

---

## Architecture

```
                         ┌─────────────────────────┐
                         │    S3 Source Bucket      │
                         │ ll-source-bucket-analytics│
                         └───────────┬─────────────┘
                                     │ .parquet upload
                                     ▼
                         ┌─────────────────────────┐
                         │    AWS Lambda            │
                         │ ll-etl-function          │
                         │ Parquet → CSV conversion │
                         │ Error → /error/ prefix   │
                         └───────────┬─────────────┘
                                     │ .csv output
                                     ▼
                         ┌─────────────────────────┐
                         │  S3 Processed Bucket     │
                         │ll-processed-bucket-analytics│
                         └─────────────────────────┘

health_products.txt ──→ [Python Cleaning] ──→ cleaned CSV ──┐
health_lapses.parquet ──→ [AWS Lambda] ──→ converted CSV ───┤
clients.csv ────────────────────────────────────────────────┤
                                                            ▼
                                                    ┌──────────────┐
                                                    │  PostgreSQL   │
                                                    │  Star Schema  │
                                                    └──────┬───────┘
                                                           │
                                                           ▼
                                                    ┌──────────────┐
                                                    │  dbt Models   │
                                                    │ dim_patients  │
                                                    │ fct_patient_  │
                                                    │ claims_summary│
                                                    └──────────────┘
```

---

## Quick Start

### Prerequisites
- Docker
- Python 3.9+
- Poetry (optional, for local development)

### 1. Start PostgreSQL

```bash
docker run --name alignd-postgres -e POSTGRES_USER=alignd_user -e POSTGRES_PASSWORD=alignd_pass -e POSTGRES_DB=alignd_db -p 5432:5432 -d postgres:15
```

### 2. Create Star Schema

```powershell
docker cp sql/ddl/star_schema.sql alignd-postgres:/tmp/star_schema.sql
docker exec alignd-postgres psql -U alignd_user -d alignd_db -f /tmp/star_schema.sql
```

### 3. Clean Health Products (Task 2)

```bash
pip install pandas pyarrow psycopg2-binary
python scripts/clean_health_products.py
```

### 4. Load Data (Task 3)

```bash
python scripts/load_to_postgres.py
```

### 5. Run SQL Transformations (Task 4)

```powershell
docker cp sql/transformations/unified_dataset.sql alignd-postgres:/tmp/unified_dataset.sql
docker exec alignd-postgres psql -U alignd_user -d alignd_db -f /tmp/unified_dataset.sql
```

### 6. Run dbt Models (Task 5)

```bash
cd dbt_project
dbt run
dbt test
```

---

## Task Summary

### Task 1: Resilient Cloud ETL (AWS)
- **Lambda:** `ll-etl-function` triggered by S3 uploads (`.parquet` suffix)
- **Conversion:** Parquet → CSV using pandas
- **Error handling:** Failed files moved to `/error/` prefix with structured JSON logging to CloudWatch
- **Buckets:** `ll-source-bucket-analytics` → `ll-processed-bucket-analytics`

### Task 2: Production-Grade Python
- **Script:** `scripts/clean_health_products.py`
- **Features:**
  - Programmatic metadata header detection (not hardcoded)
  - Programmatic delimiter detection (tests multiple delimiters)
  - Product ID casing standardization
  - Idempotent — safe to run multiple times

### Task 3: Schema Design & Modelling
- **Schema:** Star schema in PostgreSQL (`alignd` schema)
- **Tables:**
  - `dim_clients` — Patient demographics (PII flagged)
  - `dim_products` — Health insurance products
  - `dim_date` — Calendar dimension
  - `dim_policy` — Bridge table (synthetic mapping, see AD-006)
  - `fct_health_lapses` — Core fact table
  - `v_dim_patients` — Anonymized view (POPIA compliance)
- **Indexes:** All FK columns and commonly filtered columns indexed
- **Data Loading:** `scripts/load_to_postgres.py` reads from source files programmatically

### Task 4: SQL Transformation & Data Quality
- **Deduplication:** `ROW_NUMBER()` window function partitioned by `client_id`
- **Imputation:** Province-level median income
  - Median chosen over mean (income is right-skewed)
  - Group-based preserves regional patterns
  - Gauteng: 60,500 | Western Cape: 58,500 | KZN: 45,000
- **Output:** Unified analytical view joining all dimensions with fact table

### Task 5: dbt + Docker
- **Models:** 5 total (3 staging views + 2 mart tables)
  - `stg_clients` — Deduplicated, income-imputed
  - `stg_products` — Clean pass-through
  - `stg_health_lapses` — Enriched with policy bridge
  - `dim_patients` — POPIA-compliant (name excluded)
  - `fct_patient_claims_summary` — Aggregated per client with lapse rate
- **Tests:** 20 tests, all passing (unique, not_null, accepted_values)
- **Docker:** Enhanced Dockerfile with Poetry support

---

## Data Model

```
    dim_clients ──┐
                  ├──→ dim_policy ──→ fct_health_lapses
    dim_products ─┘                         │
                                            │
                                       dim_date
```

| Table | Type | Records | Grain |
|-------|------|---------|-------|
| dim_clients | Dimension | 8 | One row per client |
| dim_products | Dimension | 5 | One row per product |
| dim_date | Dimension | 121 | One row per calendar date |
| dim_policy | Bridge | 100 | One row per policy |
| fct_health_lapses | Fact | 100 | One row per policy per lapse date |

---

## POPIA & Data Privacy

This pipeline handles healthcare data subject to the Protection of Personal Information Act (POPIA).

- **PII fields** (name, income) are flagged in DDL comments
- **dim_patients** dbt model excludes patient names from the analytical layer
- **v_dim_patients** view hashes names for anonymized analytical access
- **Design principle:** Minimum necessary PII flows into analytical models

See [Technical Decisions](docs/TECHNICAL_DECISIONS.md) (AD-007) for full details.

---

## Scaling to 100x Volume

The current pipeline processes 100 records across 3 files. To scale to 10,000+ records and hundreds of files:

### Ingestion Layer
- **S3 partitioning:** Organize files by date (`/year=/month=/day=`) to enable targeted processing and reduce scan costs
- **Lambda concurrency:** Configure reserved concurrency and implement SQS dead-letter queues for failed events
- **Step Functions:** Replace single Lambda with an orchestrated workflow for multi-step ETL (validate → convert → load → notify)

### Storage Layer
- **PostgreSQL partitioning:** Partition `fct_health_lapses` by `lapse_date` (monthly) to improve query performance on time-based filters
- **Migration path:** At significant scale, consider migrating from PostgreSQL to Amazon Redshift with DIST keys on `client_id` and SORT keys on `lapse_date`
- **Incremental loading:** Replace full table loads with upsert patterns (INSERT ON CONFLICT)

### Transformation Layer
- **dbt incremental models:** Replace full table rebuilds with incremental materialization to process only new/changed records
- **dbt parallelism:** Increase thread count and optimize model DAG for parallel execution

### Orchestration
- **Airflow/Dagster:** Replace manual pipeline execution with scheduled, monitored DAGs
- **Alerting:** Add Slack/email notifications for pipeline failures
- **Data contracts:** Implement schema validation at ingestion to catch upstream changes early

### Cost Optimization
- **S3 lifecycle policies:** Move older data to S3 Glacier for cost savings
- **Lambda right-sizing:** Monitor memory usage and adjust allocation (currently 256MB, 85MB used)
- **Reserved capacity:** For predictable workloads, use Redshift reserved instances

---

## Repository Structure

```
alignd-data-engineering-assessment/
├── README.md                              ← You are here
├── Dockerfile                             ← Enhanced from provided base
├── pyproject.toml                         ← Poetry dependency management
├── .gitignore
├── data_files/
│   ├── clients.csv                        ← Source: 9 rows (1 duplicate)
│   ├── health_lapses.parquet              ← Source: 100 policy records
│   ├── health_products.txt                ← Source: pipe-delimited, metadata header
│   └── cleaned/
│       └── health_products.csv            ← Task 2 output
├── docs/
│   ├── TECHNICAL_DECISIONS.md             ← Architecture decisions log
│   └── AI_USAGE.md
├── dbt_project/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   └── models/
│       ├── staging/
│       │   ├── stg_clients.sql
│       │   ├── stg_products.sql
│       │   ├── stg_health_lapses.sql
│       │   └── schema.yml
│       └── marts/
│           ├── dim_patients.sql
│           ├── fct_patient_claims_summary.sql
│           └── schema.yml
├── lambda/
│   └── etl_handler.py                     ← Task 1: Lambda handler
├── scripts/
│   ├── clean_health_products.py           ← Task 2: Data cleaning
│   └── load_to_postgres.py                ← Task 3: Data loading
└── sql/
    ├── ddl/
    │   └── star_schema.sql                ← Task 3: Schema DDL
    └── transformations/
        └── unified_dataset.sql            ← Task 4: DQ transformations
```

---

## Tech Stack

| Tool | Usage |
|------|-------|
| **AWS Lambda** | Serverless parquet-to-CSV conversion |
| **AWS S3** | Source and processed file storage |
| **Python** | Data cleaning, loading scripts |
| **PostgreSQL** | Star schema data warehouse |
| **SQL** | DDL, transformations, data quality |
| **dbt** | Model orchestration and testing |
| **Docker** | PostgreSQL container, reproducible environment |
| **Poetry** | Python dependency management |
| **DBeaver** | Database client for development |
| **GitHub** | Version control |

---

##  Detailed Decisions

For architectural decisions, data assessment, and implementation rationale, see [Technical Decisions Record](docs/TECHNICAL_DECISIONS.md).
```


