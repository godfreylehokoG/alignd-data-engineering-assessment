"""
Task 2: (Data Cleaning)
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
            # Skip empty lines and lines starting with common metadata markers
            if not stripped or stripped.startswith("---") or stripped.startswith("#"):
                metadata_count += 1
            else:
                break
    
    logger.info(f"Detected {metadata_count} metadata line(s) to skip")
    return metadata_count