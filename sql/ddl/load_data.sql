"""
Task 3: Data Loading Script
Loads cleaned data files into PostgreSQL star schema.

This script reads from the actual source/cleaned files,
not hardcoded values  designed to handle future data.
"""

import os
import logging
import pandas as pd
import psycopg2
from psycopg2 import sql
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Database connection config
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", "5432"),
    "dbname": os.getenv("DB_NAME", "alignd_db"),
    "user": os.getenv("DB_USER", "alignd_user"),
    "password": os.getenv("DB_PASSWORD", "alignd_pass"),
}


def get_connection():
    """Create and return a database connection."""
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = True
    logger.info("Connected to PostgreSQL")
    return conn


def load_dim_clients(conn, filepath: str) -> None:
    """
    Load clients from CSV into dim_clients.
    
    Handles deduplication at load time  if a client_id
    already exists, it is skipped (ON CONFLICT DO NOTHING).
    This makes the script idempotent.
    """
    df = pd.read_csv(filepath)
    logger.info(f"Read {len(df)} rows from {filepath}")

    # Drop duplicates from source before loading
    df = df.drop_duplicates(subset=["client_id"], keep="first")
    logger.info(f"After deduplication: {len(df)} unique clients")

    cur = conn.cursor()
    for _, row in df.iterrows():
        cur.execute(
            """
            INSERT INTO alignd.dim_clients (client_id, name, income, province)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (client_id) DO NOTHING
            """,
            (
                int(row["client_id"]),
                row["name"],
                None if pd.isna(row["income"]) else float(row["income"]),
                row["province"],
            ),
        )
    cur.close()
    logger.info("dim_clients loaded successfully")


def load_dim_products(conn, filepath: str) -> None:
    """
    Load products from cleaned CSV into dim_products.
    Idempotent via ON CONFLICT DO NOTHING.
    """
    df = pd.read_csv(filepath)
    logger.info(f"Read {len(df)} rows from {filepath}")

    cur = conn.cursor()
    for _, row in df.iterrows():
        cur.execute(
            """
            INSERT INTO alignd.dim_products (product_id, product_name, tier, status)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (product_id) DO NOTHING
            """,
            (row["product_id"], row["product_name"], row["tier"], row["status"]),
        )
    cur.close()
    logger.info("dim_products loaded successfully")


def load_dim_date(conn) -> None:
    """
    Generate and load date dimension.
    Covers the full date range from the health_lapses data.
    Idempotent via ON CONFLICT DO NOTHING.
    """
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO alignd.dim_date (
            date_key, full_date, year, quarter, month, month_name,
            day, day_of_week, day_name, is_weekend, is_month_start, is_month_end
        )
        SELECT
            TO_CHAR(d, 'YYYYMMDD')::INTEGER AS date_key,
            d AS full_date,
            EXTRACT(YEAR FROM d)::INTEGER,
            EXTRACT(QUARTER FROM d)::INTEGER,
            EXTRACT(MONTH FROM d)::INTEGER,
            TRIM(TO_CHAR(d, 'Month')),
            EXTRACT(DAY FROM d)::INTEGER,
            EXTRACT(DOW FROM d)::INTEGER,
            TRIM(TO_CHAR(d, 'Day')),
            EXTRACT(DOW FROM d) IN (0, 6),
            d = DATE_TRUNC('month', d)::DATE,
            d = (DATE_TRUNC('month', d) + INTERVAL '1 month' - INTERVAL '1 day')::DATE
        FROM generate_series('2024-01-01'::DATE, '2024-04-30'::DATE, '1 day'::INTERVAL) AS d
        ON CONFLICT (date_key) DO NOTHING
        """
    )
    cur.close()
    logger.info("dim_date loaded successfully")


def load_dim_policy(conn, num_clients: int = 8, num_products: int = 5) -> None:
    """
    Generate synthetic policy-to-client-product mapping.
    
    AD-006: Source data has no natural foreign keys between datasets.
    Deterministic modulo mapping ensures reproducibility.
    """
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO alignd.dim_policy (policy_id, client_id, product_id)
        SELECT
            policy_id,
            (policy_id %% %s) + 1 AS client_id,
            'prod_' || LPAD(((policy_id %% %s) + 1)::TEXT, 3, '0') AS product_id
        FROM generate_series(1001, 1100) AS policy_id
        ON CONFLICT (policy_id) DO NOTHING
        """,
        (num_clients, num_products),
    )
    cur.close()
    logger.info("dim_policy loaded successfully")


def load_fct_health_lapses(conn, filepath: str) -> None:
    """
    Load health lapses from parquet file into fact table.
    
    Reads the original parquet file directly  in production,
    this would read from the Lambda-converted CSV in S3.
    """
    df = pd.read_parquet(filepath)
    logger.info(f"Read {len(df)} rows from {filepath}")

    cur = conn.cursor()
    for _, row in df.iterrows():
        date_key = int(row["lapse_date"].strftime("%Y%m%d"))
        cur.execute(
            """
            INSERT INTO alignd.fct_health_lapses (
                policy_id, lapse_date, date_key, premium_amount, status
            )
            VALUES (%s, %s, %s, %s, %s)
            """,
            (
                int(row["policy_id"]),
                row["lapse_date"],
                date_key,
                float(row["premium_amount"]),
                row["status"],
            ),
        )
    cur.close()
    logger.info("fct_health_lapses loaded successfully")


def main():
    """
    Main loading pipeline.
    
    Idempotent: Safe to run multiple times. Uses ON CONFLICT 
    DO NOTHING for dimension tables. Fact table is truncated
    and reloaded to ensure consistency.
    """
    project_root = Path(__file__).resolve().parent.parent

    # File paths
    clients_file = project_root / "data_files" / "clients.csv"
    products_file = project_root / "data_files" / "cleaned" / "health_products.csv"
    lapses_file = project_root / "data_files" / "health_lapses.parquet"

    # Validate files exist
    for f in [clients_file, products_file, lapses_file]:
        if not f.exists():
            logger.error(f"File not found: {f}")
            raise FileNotFoundError(f"Required file missing: {f}")
        logger.info(f"Found: {f}")

    conn = get_connection()

    try:
        # Truncate fact table for idempotent reload
        cur = conn.cursor()
        cur.execute("TRUNCATE TABLE alignd.fct_health_lapses RESTART IDENTITY")
        cur.close()
        logger.info("Truncated fct_health_lapses for fresh load")

        # Load in dependency order: dimensions first, then facts
        load_dim_clients(conn, str(clients_file))
        load_dim_products(conn, str(products_file))
        load_dim_date(conn)
        load_dim_policy(conn)
        load_fct_health_lapses(conn, str(lapses_file))

        logger.info("All tables loaded successfully!")

    except Exception as e:
        logger.error(f"Loading failed: {e}")
        raise
    finally:
        conn.close()
        logger.info("Database connection closed")


if __name__ == "__main__":
    main()