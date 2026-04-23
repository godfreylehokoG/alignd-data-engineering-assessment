-- ============================================================
-- Task 3: Star Schema Design & Modelling
-- Alignd Data Engineering Assessment
-- 
-- Schema: alignd
-- Pattern: Star Schema (Fact + Dimension tables)
-- 
-- POPIA Note: dim_clients contains PII (name, income).
-- In production, this table would have restricted access
-- and column-level encryption on sensitive fields.
-- ============================================================

-- Drop everything first for a clean start
DROP SCHEMA IF EXISTS alignd CASCADE;

-- Create schema
CREATE SCHEMA IF NOT EXISTS alignd;

-- ============================================================
-- DIMENSION TABLES
-- ============================================================

-- ------------------------------------------------------------
-- dim_clients: Patient/member demographic information
-- PII: Contains personally identifiable information
-- Access: Restricted to Data Engineers and Clinical (approved)
-- ------------------------------------------------------------
CREATE TABLE alignd.dim_clients (
    client_id       INTEGER PRIMARY KEY,
    name            VARCHAR(100) NOT NULL,      -- PII: restricted in production
    income          NUMERIC(12,2),              -- PII: restricted in production
    province        VARCHAR(50) NOT NULL
);

COMMENT ON TABLE alignd.dim_clients IS 
'Patient dimension containing PII. Access restricted per POPIA.';
COMMENT ON COLUMN alignd.dim_clients.name IS 
'PII: Patient full name. Excluded from analytical layer.';
COMMENT ON COLUMN alignd.dim_clients.income IS 
'PII: Patient income. NULL values imputed in analytical layer using province-level median.';

-- Index on province for analytical grouping and imputation joins
CREATE INDEX idx_dim_clients_province ON alignd.dim_clients(province);


-- ------------------------------------------------------------
-- dim_products: Health insurance product catalogue
-- No PII: Safe for analytical access
-- ------------------------------------------------------------
CREATE TABLE alignd.dim_products (
    product_id      VARCHAR(10) PRIMARY KEY,
    product_name    VARCHAR(100) NOT NULL,
    tier            VARCHAR(20) NOT NULL,
    status          VARCHAR(20) NOT NULL
);

COMMENT ON TABLE alignd.dim_products IS 
'Health insurance product dimension. Contains product tiers and status.';

-- Index on tier for filtering and grouping
CREATE INDEX idx_dim_products_tier ON alignd.dim_products(tier);
-- Index on status for active/inactive filtering
CREATE INDEX idx_dim_products_status ON alignd.dim_products(status);


-- ------------------------------------------------------------
-- dim_date: Calendar dimension for time-based analysis
-- Standard star schema practice for date-based reporting
-- ------------------------------------------------------------
CREATE TABLE alignd.dim_date (
    date_key        INTEGER PRIMARY KEY,        -- Format: YYYYMMDD
    full_date       DATE NOT NULL UNIQUE,
    year            INTEGER NOT NULL,
    quarter         INTEGER NOT NULL,
    month           INTEGER NOT NULL,
    month_name      VARCHAR(20) NOT NULL,
    day             INTEGER NOT NULL,
    day_of_week     INTEGER NOT NULL,
    day_name        VARCHAR(20) NOT NULL,
    is_weekend      BOOLEAN NOT NULL,
    is_month_start  BOOLEAN NOT NULL,
    is_month_end    BOOLEAN NOT NULL
);

COMMENT ON TABLE alignd.dim_date IS 
'Calendar dimension enabling time-based analysis without date function overhead in queries.';

-- Index on full_date for joins from fact table
CREATE INDEX idx_dim_date_full_date ON alignd.dim_date(full_date);
-- Index on year/month for partition-style filtering
CREATE INDEX idx_dim_date_year_month ON alignd.dim_date(year, month);


-- ------------------------------------------------------------
-- dim_policy: Bridge table linking clients to products
-- 
-- AD-006: The source datasets have no natural foreign keys
-- between them. In production, this mapping would exist in
-- the source system. Here we use deterministic assignment
-- to demonstrate the star schema relationships.
--
-- Mapping logic:
--   client_id  = (policy_id % 8) + 1
--   product_id = product_list[policy_id % 5]
-- ------------------------------------------------------------
CREATE TABLE alignd.dim_policy (
    policy_id       INTEGER PRIMARY KEY,
    client_id       INTEGER NOT NULL,
    product_id      VARCHAR(10) NOT NULL,
    CONSTRAINT fk_policy_client 
        FOREIGN KEY (client_id) REFERENCES alignd.dim_clients(client_id),
    CONSTRAINT fk_policy_product 
        FOREIGN KEY (product_id) REFERENCES alignd.dim_products(product_id)
);

COMMENT ON TABLE alignd.dim_policy IS 
'Bridge dimension linking policies to clients and products. See AD-006 in TDR.';

-- Indexes on foreign keys for join performance
CREATE INDEX idx_dim_policy_client ON alignd.dim_policy(client_id);
CREATE INDEX idx_dim_policy_product ON alignd.dim_policy(product_id);


-- ============================================================
-- FACT TABLE
-- ============================================================

-- ------------------------------------------------------------
-- fct_health_lapses: Core fact table
-- Grain: One row per policy per lapse date
-- Measures: premium_amount
-- ------------------------------------------------------------
CREATE TABLE alignd.fct_health_lapses (
    lapse_id            SERIAL PRIMARY KEY,
    policy_id           INTEGER NOT NULL,
    lapse_date          DATE NOT NULL,
    date_key            INTEGER NOT NULL,
    premium_amount      NUMERIC(10,2) NOT NULL,
    status              VARCHAR(20) NOT NULL,
    CONSTRAINT fk_lapse_policy 
        FOREIGN KEY (policy_id) REFERENCES alignd.dim_policy(policy_id),
    CONSTRAINT fk_lapse_date 
        FOREIGN KEY (date_key) REFERENCES alignd.dim_date(date_key)
);

COMMENT ON TABLE alignd.fct_health_lapses IS 
'Fact table recording health policy lapse events. Grain: one row per policy per lapse date.';

-- Indexes on foreign keys for join performance
CREATE INDEX idx_fct_lapses_policy ON alignd.fct_health_lapses(policy_id);
CREATE INDEX idx_fct_lapses_date_key ON alignd.fct_health_lapses(date_key);
-- Index on status for filtering Active/Pending/Lapsed
CREATE INDEX idx_fct_lapses_status ON alignd.fct_health_lapses(status);
-- Composite index for common analytical query pattern
CREATE INDEX idx_fct_lapses_date_status ON alignd.fct_health_lapses(lapse_date, status);


-- ============================================================
-- ANONYMIZED VIEW (POPIA Compliance)
-- ============================================================

-- ------------------------------------------------------------
-- v_dim_patients: Anonymized patient view for analytical layer
-- Strips PII (name) and provides hashed identifier
-- Access: Available to Analysts, Finance, Executives
-- ------------------------------------------------------------
CREATE VIEW alignd.v_dim_patients AS
SELECT
    client_id,
    md5(name) AS patient_hash,      -- One-way hash, not reversible
    income,
    province
FROM alignd.dim_clients;

COMMENT ON VIEW alignd.v_dim_patients IS 
'Anonymized patient view for analytical use. Name is hashed per POPIA. For raw PII, contact Data Engineering.';


-- ============================================================
-- SCHEMA DIAGRAM (for reference)
-- ============================================================
--
--     dim_clients ──┐
--                   ├──→ dim_policy ──→ fct_health_lapses
--     dim_products ─┘                         │
--                                             │
--                                       dim_date
--
-- ============================================================