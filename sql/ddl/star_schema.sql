-- POPIA Note: dim_clients contains PII (name, income).
-- In production, this table would have restricted access
-- and column-level encryption on sensitive fields.
-- Analytical queries should join through dim_policy 
-- using client_id only where necessary.

CREATE TABLE alignd.dim_clients (
    client_id     INTEGER PRIMARY KEY,
    name          VARCHAR(100) NOT NULL,  -- PII: restricted access in production
    income        NUMERIC(12,2),          -- PII: restricted access in production
    province      VARCHAR(50) NOT NULL
);