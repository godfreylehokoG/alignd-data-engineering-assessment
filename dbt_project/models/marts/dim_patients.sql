-- ============================================================
-- Dimension: Patients
-- Source: stg_clients
--
-- POPIA Compliance: This model deliberately excludes the
-- 'name' field to minimize PII exposure in the analytical
-- layer. Analysts can request PII access through the
-- Data Engineering team if approved.
--
-- Grain: One row per unique patient
-- ============================================================

SELECT
    client_id,
    -- AD-007: name intentionally excluded (POPIA compliance)
    income,
    income_was_imputed,
    province
FROM {{ ref('stg_clients') }}