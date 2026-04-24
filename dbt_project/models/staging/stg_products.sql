-- ============================================================
-- Staging: Products
-- Source: alignd.dim_products
--
-- Product IDs already standardized to lowercase in Task 2
-- ============================================================

SELECT
    product_id,
    product_name,
    tier,
    status AS product_status
FROM {{ source('alignd', 'dim_products') }}