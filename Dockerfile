# ============================================================
# Dockerfile: Alignd Data Engineering Assessment
#
# Builds a containerized environment with Python, dbt,
# and all dependencies needed to run the pipeline.
# ============================================================

FROM python:3.9-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy project files
COPY . /app

# Install Python dependencies
RUN pip install --no-cache-dir \
    pandas \
    boto3 \
    pyarrow \
    psycopg2-binary \
    dbt-postgres

# Set dbt profiles directory
ENV DBT_PROFILES_DIR=/app/dbt_project

# Default command
CMD ["bash"]