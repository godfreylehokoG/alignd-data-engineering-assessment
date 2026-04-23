-- dim_patients deliberately excludes the 'name' field
-- to minimize PII exposure in the analytical layer.
-- Analysts can join back to source if PII access is approved.

SELECT
    client_id,
    -- name intentionally excluded (POPIA compliance)
    income,
    province
FROM {{ ref('stg_clients') }}