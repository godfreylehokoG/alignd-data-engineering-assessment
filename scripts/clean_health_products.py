"""
Task 2: Production-Grade Python (Data Cleaning)
Processes health_products.txt into a clean CSV.
Handles metadata headers, pipe delimiters, and inconsistent casing.
"""

import os
import csv
import logging
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def detect_metadata_lines(filepath: str) -> int:
    """
    Programmatically detect how many lines at the top of the file
    are metadata (non-data) lines.
    
    Strategy: Metadata lines typically start with special characters
    like '---', '#', or '//' and don't contain the data delimiter.
    """
    metadata_count = 0
    with open(filepath, "r") as f:
        for line in f:
            stripped = line.strip()
            if not stripped or stripped.startswith("---") or stripped.startswith("#"):
                metadata_count += 1
            else:
                break

    logger.info(f"Detected {metadata_count} metadata line(s) to skip")
    return metadata_count


def detect_delimiter(filepath: str, skip_lines: int = 0) -> str:
    """
    Programmatically detect the delimiter used in the data file.
    
    Strategy: Check first two data lines for common delimiters.
    The delimiter that produces consistent column counts wins.
    """
    common_delimiters = ["|", ",", "\t", ";"]

    with open(filepath, "r") as f:
        for _ in range(skip_lines):
            next(f)
        first_line = f.readline().strip()
        second_line = f.readline().strip()

    best_delimiter = ","
    best_count = 0

    for d in common_delimiters:
        first_count = len(first_line.split(d))
        second_count = len(second_line.split(d))

        if first_count == second_count and first_count > best_count:
            best_count = first_count
            best_delimiter = d

    logger.info(f"Detected delimiter: '{best_delimiter}' producing {best_count} columns")
    return best_delimiter


def infer_column_names(num_columns: int) -> list:
    """
    Infer column names based on column count.
    
    In production, this would be driven by a schema registry
    or data catalog. Here we use positional inference from 
    the data structure.
    """
    column_mapping = {
        4: ["product_id", "product_name", "tier", "status"],
        3: ["product_id", "product_name", "status"],
        5: ["product_id", "product_name", "tier", "status", "effective_date"],
    }

    if num_columns in column_mapping:
        columns = column_mapping[num_columns]
        logger.info(f"Inferred column names: {columns}")
        return columns

    columns = [f"column_{i+1}" for i in range(num_columns)]
    logger.warning(f"Could not infer names. Using generic: {columns}")
    return columns


def clean_data(rows: list, columns: list) -> list:
    """
    Apply cleaning transformations to raw data rows.
    
    Transformations:
    - Strip whitespace from all fields
    - Standardize product_id to lowercase (AD-004)
    - Validate row length matches expected column count
    """
    cleaned = []
    expected_cols = len(columns)

    for i, row in enumerate(rows):
        if len(row) != expected_cols:
            logger.warning(
                f"Row {i+1} skipped: expected {expected_cols} columns, "
                f"got {len(row)}  data: {row}"
            )
            continue

        cleaned_row = [field.strip() for field in row]

        # AD-004: Standardize product_id to lowercase
        if "product_id" in columns:
            id_index = columns.index("product_id")
            cleaned_row[id_index] = cleaned_row[id_index].lower()

        cleaned.append(cleaned_row)

    logger.info(f"Cleaned {len(cleaned)} of {len(rows)} rows successfully")
    return cleaned


def clean_health_products(input_path: str, output_path: str) -> None:
    """
    Main cleaning pipeline for health_products.txt.
    
    This function is idempotent  running it multiple times
    produces the same output. The output file is fully
    overwritten on each run to ensure consistency.
    """
    logger.info(f"Starting cleaning pipeline for {input_path}")

    # Validate input
    if not os.path.exists(input_path):
        logger.error(f"Input file not found: {input_path}")
        raise FileNotFoundError(f"Input file not found: {input_path}")

    # Step 1: Detect metadata lines
    skip_lines = detect_metadata_lines(input_path)

    # Step 2: Detect delimiter
    delimiter = detect_delimiter(input_path, skip_lines)

    # Step 3: Read data rows
    rows = []
    with open(input_path, "r") as f:
        for _ in range(skip_lines):
            next(f)
        for line in f:
            stripped = line.strip()
            if stripped:
                rows.append(stripped.split(delimiter))

    if not rows:
        raise ValueError("No data rows found after metadata lines")

    # Step 4: Infer column names
    num_columns = len(rows[0])
    columns = infer_column_names(num_columns)

    # Step 5: Clean and standardize
    cleaned_rows = clean_data(rows, columns)

    # Step 6: Write to CSV (overwrite ensures idempotency)
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(columns)
        writer.writerows(cleaned_rows)

    logger.info(f"Successfully wrote {len(cleaned_rows)} rows to {output_path}")


if __name__ == "__main__":
    project_root = Path(__file__).resolve().parent.parent
    input_file = project_root / "data_files" / "health_products.txt"
    output_file = project_root / "data_files" / "cleaned" / "health_products.csv"

    clean_health_products(
        input_path=str(input_file),
        output_path=str(output_file)
    )