-- ============================================================
-- Task 4: SQL Transformation & Data Quality
-- Alignd Data Engineering Assessment
--
-- Purpose: Create a unified analytical dataset from the
-- star schema with deduplication and imputation applied.
-- ============================================================


-- ============================================================
-- STEP 1: DEDUPLICATION
-- ============================================================

-- ------------------------------------------------------------
-- Deduplicate dim_clients using ROW_NUMBER()
--
-- AD-003: Source data contains exact duplicate for client_id=4.
-- We use ROW_NUMBER() partitioned by client_id to identify
-- and keep only the first occurrence of each client.
--
-- Why ROW_NUMBER over DISTINCT:
--   - Handles partial duplicates (same ID, different values)
--   - Gives explicit control over which row to keep
--   - Industry standard for deduplication in data pipelines
-- ------------------------------------------------------------
CREATE OR REPLACE VIEW alignd.v_deduplicated_clients AS
WITH ranked_clients AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY client_id
            ORDER BY client_id
        ) AS rn
    FROM alignd.dim_clients
)
SELECT
    client_id,
    name,
    income,
    province
FROM ranked_clients
WHERE rn = 1;


-- ============================================================
-- STEP 2: NULL INCOME IMPUTATION
-- ============================================================

-- ------------------------------------------------------------
-- Impute NULL income values using province-level median
--
-- AD-002: Strategy  Province-Level Median
--
-- Justification:
--   1. Income varies significantly by region in South Africa.
--      Gauteng and Western Cape typically have higher median
--      incomes than KwaZulu-Natal.
--   2. Median is preferred over mean because income 
--      distributions are typically right-skewed  a few 
--      high earners would inflate the mean.
--   3. Mode is inappropriate for continuous numerical data.
--   4. Group-based imputation preserves regional patterns
--      better than a single global statistic.
--
-- Province medians from our data:
--   Gauteng:      MEDIAN(50000, 71000) = 60500
--   Western Cape: MEDIAN(62000, 55000) = 58500
--   KZN:          MEDIAN(45000)        = 45000
--
-- Imputed values:
--   client_id 3 (Bob Hill, Gauteng)         → 60500
--   client_id 5 (Charlie Day, Western Cape) → 58500
--   client_id 8 (Grace Garden, KZN)         → 45000
-- ------------------------------------------------------------
CREATE OR REPLACE VIEW alignd.v_imputed_clients AS
WITH province_medians AS (
    SELECT
        province,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY income) AS median_income
    FROM alignd.v_deduplicated_clients
    WHERE income IS NOT NULL
    GROUP BY province
)
SELECT
    c.client_id,
    c.name,
    COALESCE(c.income, pm.median_income) AS income,
    c.income IS NULL AS income_was_imputed,
    c.province
FROM alignd.v_deduplicated_clients c
LEFT JOIN province_medians pm
    ON c.province = pm.province;


-- ============================================================
-- STEP 3: UNIFIED ANALYTICAL DATASET
-- ============================================================

-- ------------------------------------------------------------
-- Unified dataset joining all dimensions with fact table
--
-- This is the analytics-ready dataset that downstream
-- consumers (analysts, dbt models, dashboards) would use.
-- It combines:
--   - Imputed client data (no NULLs, no duplicates)
--   - Product information
--   - Policy mappings
--   - Health lapse events
--   - Date dimensions
-- ------------------------------------------------------------
CREATE OR REPLACE VIEW alignd.v_unified_dataset AS
SELECT
    -- Client dimensions
    c.client_id,
    c.income,
    c.income_was_imputed,
    c.province,

    -- Policy dimensions
    p.policy_id,

    -- Product dimensions
    pr.product_id,
    pr.product_name,
    pr.tier,
    pr.status AS product_status,

    -- Fact measures
    f.lapse_id,
    f.lapse_date,
    f.premium_amount,
    f.status AS lapse_status,

    -- Date dimensions
    d.year,
    d.quarter,
    d.month,
    d.month_name,
    d.day_name,
    d.is_weekend

FROM alignd.fct_health_lapses f
INNER JOIN alignd.dim_policy p
    ON f.policy_id = p.policy_id
INNER JOIN alignd.v_imputed_clients c
    ON p.client_id = c.client_id
INNER JOIN alignd.dim_products pr
    ON p.product_id = pr.product_id
INNER JOIN alignd.dim_date d
    ON f.date_key = d.date_key;


-- ============================================================
-- VALIDATION QUERIES
-- ============================================================

-- ------------------------------------------------------------
-- Verify: No duplicate client_ids after deduplication
-- ------------------------------------------------------------
-- SELECT client_id, COUNT(*)
-- FROM alignd.v_deduplicated_clients
-- GROUP BY client_id
-- HAVING COUNT(*) > 1;
-- Expected: 0 rows

-- ------------------------------------------------------------
-- Verify: No NULL incomes after imputation
-- ------------------------------------------------------------
-- SELECT COUNT(*)
-- FROM alignd.v_imputed_clients
-- WHERE income IS NULL;
-- Expected: 0

-- ------------------------------------------------------------
-- Verify: Unified dataset row count matches fact table
-- ------------------------------------------------------------
-- SELECT COUNT(*) FROM alignd.v_unified_dataset;
-- Expected: 100

-- ============================================================
-- END OF TRANSFORMATION SCRIPT
-- ============================================================