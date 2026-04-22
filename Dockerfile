FROM python:3.9-slim
WORKDIR /app
COPY . /app
RUN pip install pandas boto3 pyarrow dbt-postgres
CMD ["bash"]