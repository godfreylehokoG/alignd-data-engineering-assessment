# Technical Decisions Record (TDR)
## Alignd Data Engineering Assessment: Resilient Pipelines

> This document tracks architectural decisions, design rationale, 
> and implementation notes for each task. It serves as a living 
> reference throughout development.

---

## 📋 Project Context

| Item | Detail |
|------|--------|
| **Role** | Data Engineer |
| **Company** | Alignd — Healthcare financing solutions |
| **Domain** | Health insurance data (clients, products, policy lapses) |
| **Tech Stack** | AWS, Python, PostgreSQL, dbt, Docker, Poetry |
| **Data Volume** | 8 clients, 5 products, 100 policy records |

---

## 📊 Data Assessment

### clients.csv
- **Records:** 9 rows (8 unique clients)
- **Issues Found:**
  - Duplicate: `client_id = 4` (Alice Vane) — exact duplicate row
  - NULL income: `client_id` 3, 5, 8 (37.5% missing)
- **Provinces:** Gauteng, Western Cape, KZN

### health_products.txt
- **Records:** 5 products
- **Issues Found:**
  - Metadata header line (starts with `---`)
  - Pipe-delimited with no column headers
  - Inconsistent product_id casing (`prod_001` vs `PROD_002`)

### health_lapses.parquet
- **Records:** 100 policies (ID range: 1001–1100)
- **Date Range:** 2024-01-01 to 2024-04-09
- **Columns:** policy_id, lapse_date, premium_amount, status
- **Status Values:** Active, Pending, Lapsed
- **Issues Found:**
  - No foreign keys to clients or products
  - Requires synthetic relationship mapping

---

## 🏗️ Architecture Decisions

### AD-001: Star Schema Design

**Decision:** Create a bridge table (`dim_policy`) to link clients, 
products, and health lapses since no natural foreign keys exist 
between datasets.

**Rationale:** In production, a policy table linking clients to 
products would exist in the source system. For this assessment, 
a synthetic but realistic mapping demonstrates the star schema 
design pattern without fabricating false data relationships.

**Schema:**
```
dim_clients ──┐
              ├──→ dim_policy ──→ fct_health_lapses
dim_products ─┘
```

---

### AD-002: NULL Income Imputation Strategy

**Decision:** Use province-level median income for imputation.

**Rationale:**
- Income varies significantly by region in South Africa
- Median is robust against outliers (income is typically right-skewed)
- Group-based imputation preserves regional patterns better than 
  a global statistic

| Province | Known Incomes | Median |
|----------|--------------|--------|
| Gauteng | 50000, 71000 | 60500 |
| Western Cape | 62000, 55000 | 58500 |
| KZN | 45000 | 45000 |

**Clients imputed:**
- `client_id 3` (Bob Hill, Gauteng) → 60500
- `client_id 5` (Charlie Day, Western Cape) → 58500
- `client_id 8` (Grace Garden, KZN) → 45000

---

### AD-003: Deduplication Strategy

**Decision:** Use `ROW_NUMBER()` window function partitioned by 
`client_id` to identify and remove exact duplicates.

**Rationale:** 
- Window functions are the industry standard for deduplication
- Keeps the first occurrence based on natural row order
- Non-destructive — original data is preserved in raw tables

---

### AD-004: Product ID Standardization

**Decision:** Normalize all product IDs to lowercase.

**Rationale:** 
- Source data contains mixed casing (`prod_001` vs `PROD_002`)
- Lowercase is the PostgreSQL convention for identifiers
- Consistent casing prevents join failures downstream

---

### AD-005: Metadata Header Detection

**Decision:** Programmatically detect and skip metadata lines 
rather than hardcoding `skiprows=1`.

**Rationale:**
- Future files may have multiple metadata lines
- Detection logic checks for `---` prefix pattern
- More resilient than assuming a fixed header count

---

### AD-006: Bridge Table for Missing Relationships

**Decision:** Create `dim_policy` with synthetic client-product 
mappings using deterministic assignment.

**Rationale:**
- `health_lapses.parquet` has no `client_id` or `product_id`
- A deterministic mapping (modulo-based) ensures reproducibility
- Documented clearly so reviewers understand this is a design 
  choice, not a data assumption

**Mapping logic:**
```python
client_id  = (policy_id % num_clients) + 1
product_id = products[(policy_id % num_products)]
```

---

## 📁 Task Implementation Notes

### Task 1: Resilient Cloud ETL (AWS)
- **Lambda Name:** `ll-etl-function`
- **Source Bucket:** `ll-source-bucket-analytics`
- **Processed Bucket:** `ll-processed-bucket-analytics`
- **Error Handling:** Failed files moved to `/error/` prefix
- **Logging:** Structured JSON to CloudWatch

### Task 2: Production-Grade Python
- **Script:** `scripts/clean_health_products.py`
- **Input:** `data_files/health_products.txt`
- **Output:** `data_files/cleaned/health_products.csv`
- **Key Features:** 
  - Programmatic metadata detection
  - Programmatic delimiter detection
  - Idempotent (safe to run multiple times)

### Task 3: Schema Design & Modelling
- **DDL Location:** `sql/ddl/star_schema.sql`
- **Schema:** `alignd`
- **Tables:** `dim_clients`, `dim_products`, `dim_policy`, 
  `dim_date`, `fct_health_lapses`
- **Indexes:** On all foreign key / join columns

### Task 4: SQL Transformation & Data Quality
- **Scripts Location:** `sql/transformations/`
- **Dedup Method:** `ROW_NUMBER()` window function
- **Imputation Method:** Province-level median

### Task 5: dbt + Docker
- **Models:** `dim_patients`, `fct_patient_claims_summary`
- **Tests:** `unique`, `not_null` on key columns
- **Environment:** Docker + Poetry

---

## 🔄 Scaling Strategy (100x Volume)

*To be completed after implementation — will address:*
- S3 partitioning by date
- Lambda concurrency and Step Functions
- PostgreSQL partitioning / migration to Redshift
- dbt incremental models
- Orchestration with Airflow/Dagster

---

## 📝 Change Log

| Date | Change | Task |
|------|--------|------|
| 2024-04-23 | Initial data assessment complete | All |
| 2024-04-23 | Task 2 script complete | Task 2 |
| 2024-04-23 | Task 1 ETL handler complete | Task 1 |
| 2024-04-23 | Task 3 Star Schema DDL complete | Task 3 |
| 2024-04-23 | Task 4 SQL transformations complete | Task 4 |
| 2024-04-23 | Task 5 dbt models complete | Task 5 |
| 2024-04-23 | Project implementation finalized | All |
| | | |
