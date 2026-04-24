FROM python:3.9-slim
WORKDIR /app
# Install system dependencies for psycopg2
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*
# Copy dependency files first (Docker layer caching)
COPY pyproject.toml poetry.lock* /app/
# Install dependencies via Poetry (no virtualenv in container)
RUN poetry config virtualenvs.create false \
    && poetry install --no-interaction --no-ansi
COPY . /app
RUN pip install pandas boto3 pyarrow dbt-postgres poetry
# Set dbt profiles directory
ENV DBT_PROFILES_DIR=/app/dbt_project
CMD ["bash"]