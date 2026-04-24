-- ============================================================
-- Staging: Clients
-- Source: alignd.dim_clients
--
-- Deduplicates by client_id and imputes NULL income
-- using province-level median (see AD-002, AD-003 in TDR)
-- ============================================================

WITH deduplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY client_id
            ORDER BY client_id
        ) AS rn
    FROM {{ source('alignd', 'dim_clients') }}
),

unique_clients AS (
    SELECT
        client_id,
        name,
        income,
        province
    FROM deduplicated
    WHERE rn = 1
),

-- AD-002: Province-level median for NULL income imputation
-- Median chosen over mean because income is right-skewed
province_medians AS (
    SELECT
        province,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY income) AS median_income
    FROM unique_clients
    WHERE income IS NOT NULL
    GROUP BY province
)

SELECT
    c.client_id,
    -- PII: name excluded from downstream models (POPIA)
    c.name,
    COALESCE(c.income, pm.median_income) AS income,
    c.income IS NULL AS income_was_imputed,
    c.province
FROM unique_clients c
LEFT JOIN province_medians pm
    ON c.province = pm.province