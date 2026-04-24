-- ============================================================
-- Fact: Patient Claims Summary
-- Sources: stg_health_lapses, stg_clients, stg_products
--
-- Grain: One row per client
-- Measures: Total premiums, policy count, lapse count,
--           average premium, latest lapse date
--
-- This model provides a single view of each patient's
-- claims activity for downstream reporting and analytics.
-- ============================================================

WITH lapse_details AS (
    SELECT
        l.client_id,
        l.policy_id,
        l.premium_amount,
        l.lapse_status,
        l.lapse_date,
        l.product_id
    FROM {{ ref('stg_health_lapses') }} l
),

-- Aggregate to one row per client
patient_summary AS (
    SELECT
        client_id,

        -- Policy metrics
        COUNT(DISTINCT policy_id) AS total_policies,
        COUNT(*) AS total_claims,

        -- Premium metrics
        SUM(premium_amount) AS total_premiums,
        AVG(premium_amount) AS avg_premium,
        MIN(premium_amount) AS min_premium,
        MAX(premium_amount) AS max_premium,

        -- Lapse metrics
        SUM(CASE WHEN lapse_status = 'Lapsed' THEN 1 ELSE 0 END) AS lapsed_count,
        SUM(CASE WHEN lapse_status = 'Active' THEN 1 ELSE 0 END) AS active_count,
        SUM(CASE WHEN lapse_status = 'Pending' THEN 1 ELSE 0 END) AS pending_count,

        -- Date metrics
        MIN(lapse_date) AS first_claim_date,
        MAX(lapse_date) AS latest_claim_date,

        -- Product diversity
        COUNT(DISTINCT product_id) AS distinct_products

    FROM lapse_details
    GROUP BY client_id
)

SELECT
    ps.client_id,
    p.income,
    p.province,
    ps.total_policies,
    ps.total_claims,
    ps.total_premiums,
    ROUND(ps.avg_premium, 2) AS avg_premium,
    ps.min_premium,
    ps.max_premium,
    ps.lapsed_count,
    ps.active_count,
    ps.pending_count,
    ps.first_claim_date,
    ps.latest_claim_date,
    ps.distinct_products,

    -- Derived: Lapse rate per client
    ROUND(
        ps.lapsed_count::NUMERIC / NULLIF(ps.total_claims, 0) * 100, 2
    ) AS lapse_rate_pct

FROM patient_summary ps
INNER JOIN {{ ref('dim_patients') }} p
    ON ps.client_id = p.client_id