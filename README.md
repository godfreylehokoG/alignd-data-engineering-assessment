# Alignd Data Engineering Assessment: Resilient Pipelines

## 👤 Candidate
**Lehlohonolo Lehoko**

---

## 📖 Overview
End-to-end data pipeline for processing healthcare data — from 
raw file ingestion through cloud ETL, data cleaning, warehouse 
modelling, quality checks, and automated transformations.

## 🏗️ Architecture

```
health_products.txt ──→ [Python Cleaning] ──→ cleaned CSV ──┐
health_lapses.parquet ──→ [AWS Lambda] ──→ converted CSV ───┤
clients.csv ────────────────────────────────────────────────┤
                                                            ▼
                                                    [PostgreSQL]
                                                    Star Schema
                                                            │
                                                            ▼
                                                    [dbt Models]
                                                  dim_patients
                                          fct_patient_claims_summary
```

## 🚀 Quick Start

```bash
# Clone the repo
git clone <repo-url>
cd alignd-data-engineering-assessment

# Start PostgreSQL
docker run --name alignd-postgres -e POSTGRES_USER=alignd_user -e POSTGRES_PASSWORD=alignd_pass -e POSTGRES_DB=alignd_db -p 5432:5432 -d postgres:15

# Run data cleaning (Task 2)
python scripts/clean_health_products.py

# Run dbt models (Task 5)
docker build -t alignd-dbt .
docker run alignd-dbt dbt run
docker run alignd-dbt dbt test
```

## 📊 Data Model
*Star schema diagram and details in [Technical Decisions](docs/TECHNICAL_DECISIONS.md)*

## 📈 Scaling to 100x Volume
*Detailed in [Technical Decisions](docs/TECHNICAL_DECISIONS.md#-scaling-strategy-100x-volume)*

## 📁 Repository Structure
```
├── README.md
├── docs/
│   └── TECHNICAL_DECISIONS.md
├── scripts/
│   └── clean_health_products.py
├── lambda/
│   └── etl_handler.py
├── sql/
│   └── ddl/
│       └── star_schema.sql
│   └── transformations/
│       └── unified_dataset.sql
├── dbt_project/
│   ├── dbt_project.yml
│   ├── models/
│   │   ├── dim_patients.sql
│   │   ├── fct_patient_claims_summary.sql
│   │   └── schema.yml
├── data_files/
│   ├── clients.csv
│   ├── health_lapses.parquet
│   └── health_products.txt
├── Dockerfile
└── .gitignore
```