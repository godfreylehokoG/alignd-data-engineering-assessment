"""
Task 1: Resilient Cloud ETL (AWS Lambda)
Converts health_lapses.parquet to CSV on S3 upload.

Trigger: S3 PUT event on ll-source-bucket-analytics (.parquet suffix)
Output:  CSV written to ll-processed-bucket-analytics/processed/
Error:   Failed files moved to /error/ prefix with structured CloudWatch logging
"""

import json
import logging
import os
import boto3
import pandas as pd
from io import BytesIO, StringIO
from datetime import datetime

# Configure structured logging for CloudWatch
logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3_client = boto3.client("s3")

PROCESSED_BUCKET = os.environ.get("PROCESSED_BUCKET", "ll-processed-bucket-analytics")


def lambda_handler(event, context):
    """
    AWS Lambda handler triggered by S3 upload events.

    Converts .parquet files to .csv and writes to processed bucket.
    On failure, moves source file to /error/ prefix and logs
    structured event to CloudWatch.
    """
    for record in event.get("Records", []):
        source_bucket = record["s3"]["bucket"]["name"]
        source_key = record["s3"]["object"]["key"]

        # Only process .parquet files
        if not source_key.endswith(".parquet"):
            logger.info(json.dumps({
                "event": "skipped_non_parquet",
                "source_key": source_key,
                "timestamp": datetime.utcnow().isoformat()
            }))
            continue

        logger.info(json.dumps({
            "event": "file_received",
            "source_bucket": source_bucket,
            "source_key": source_key,
            "timestamp": datetime.utcnow().isoformat()
        }))

        try:
            # Step 1: Download parquet file from S3
            response = s3_client.get_object(Bucket=source_bucket, Key=source_key)
            parquet_content = response["Body"].read()

            # Step 2: Convert parquet to DataFrame
            df = pd.read_parquet(BytesIO(parquet_content))

            # Step 3: Convert DataFrame to CSV
            csv_buffer = StringIO()
            df.to_csv(csv_buffer, index=False)

            # Step 4: Upload CSV to processed bucket
            output_key = source_key.replace(".parquet", ".csv")
            s3_client.put_object(
                Bucket=PROCESSED_BUCKET,
                Key=f"processed/{output_key}",
                Body=csv_buffer.getvalue(),
                ContentType="text/csv"
            )

            logger.info(json.dumps({
                "event": "conversion_success",
                "source_key": source_key,
                "output_bucket": PROCESSED_BUCKET,
                "output_key": f"processed/{output_key}",
                "rows_converted": len(df),
                "columns": list(df.columns),
                "timestamp": datetime.utcnow().isoformat()
            }))

        except Exception as e:
            # Move failed file to /error/ prefix
            error_key = f"error/{source_key}"

            logger.error(json.dumps({
                "event": "conversion_failed",
                "source_key": source_key,
                "error": str(e),
                "error_type": type(e).__name__,
                "timestamp": datetime.utcnow().isoformat()
            }))

            try:
                s3_client.copy_object(
                    Bucket=source_bucket,
                    CopySource={"Bucket": source_bucket, "Key": source_key},
                    Key=error_key
                )
                s3_client.delete_object(
                    Bucket=source_bucket,
                    Key=source_key
                )

                logger.info(json.dumps({
                    "event": "file_moved_to_error",
                    "source_key": source_key,
                    "error_key": error_key,
                    "timestamp": datetime.utcnow().isoformat()
                }))

            except Exception as move_error:
                logger.error(json.dumps({
                    "event": "error_handling_failed",
                    "source_key": source_key,
                    "original_error": str(e),
                    "move_error": str(move_error),
                    "timestamp": datetime.utcnow().isoformat()
                }))

    return {
        "statusCode": 200,
        "body": json.dumps("ETL processing complete")
    }