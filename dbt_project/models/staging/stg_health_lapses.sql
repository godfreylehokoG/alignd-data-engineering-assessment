SELECT
    f.lapse_id,
    f.policy_id,
    p.client_id,
    p.product_id,
    f.lapse_date,
    f.date_key,
    f.premium_amount,
    f.status AS lapse_status
FROM {{ source('alignd', 'fct_health_lapses') }} f
INNER JOIN {{ source('alignd', 'dim_policy') }} p
    ON f.policy_id = p.policy_id