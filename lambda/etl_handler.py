import json
import logging
import os
import boto3
import pandas as pd
from io import BytesIO, StringIO

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3_client = boto3.client('s3')

PROCESSED_BUCKET = os.environ.get('PROCESSED_BUCKET', 'll-processed-bucket-analytics')

def lambda_handler(event, context):
    """
    AWS Lambda handler for processing Parquet files from S3 and converting to CSV.
    """
    for record in event.get('Records', []):
        source_bucket = record['s3']['bucket']['name']
        source_key = record['s3']['object']['key']
        
        # Only process .parquet files
        if not source_key.endswith('.parquet'):
            logger.info(f"Skipping non-parquet file: {source_key}")
            continue
            
        logger.info(f"Processing file: s3://{source_bucket}/{source_key}")
        
        try:
            # 1. Read Parquet from S3
            response = s3_client.get_object(Bucket=source_bucket, Key=source_key)
            parquet_content = response['Body'].read()
            
            # 2. Convert to DataFrame
            df = pd.read_parquet(BytesIO(parquet_content))
            
            # 3. Convert to CSV
            csv_buffer = StringIO()
            df.to_csv(csv_buffer, index=False)
            
            # 4. Upload CSV to Processed bucket
            output_key = source_key.replace('.parquet', '.csv')
            s3_client.put_object(
                Bucket=PROCESSED_BUCKET,
                Key=f"processed/{output_key}",
                Body=csv_buffer.getvalue()
            )
            
            logger.info(f"Successfully converted and uploaded to s3://{PROCESSED_BUCKET}/processed/{output_key}")
            
        except Exception as e:
            logger.error(f"Error processing {source_key}: {str(e)}")
            
            # Move failed file to error prefix
            try:
                error_key = f"error/{source_key}"
                s3_client.copy_object(
                    Bucket=source_bucket,
                    CopySource={'Bucket': source_bucket, 'Key': source_key},
                    Key=error_key
                )
                logger.info(f"Moved failed file to {error_key}")
            except Exception as copy_err:
                logger.error(f"Failed to move file to error prefix: {str(copy_err)}")
                
    return {
        'statusCode': 200,
        'body': json.dumps('ETL processing job complete')
    }
