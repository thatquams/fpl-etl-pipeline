import logging

# Configure global logging

# Logs will include timestamp, log level, and message.
# INFO level is suitable for pipeline execution visibility.
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Create a module-level logger
logger = logging.getLogger(__name__)

    
def upload_data_to_s3(data, season: int, bucket: str, output_key: str="fpl_fixtures_history_data/"):
    
    import json
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook
    
    try:
    
        s3 = S3Hook(aws_conn_id="fpl_aws_conn")

        # s3://fpl-data-lake/fpl_fixtures_history_data/
        # bucket = "fpl-data-lake"
        # input = "fpl_fixtures_history_data"

        s3.load_string(
            string_data=json.dumps(data),
            key=f"{output_key}_{season}.json",
            bucket_name=bucket,
            replace=True
        )
        logger.info(f"Data successfully uploaded to s3://{bucket}/{output_key}")
        
    except Exception as e:
        logger.error(f"Failed to upload data to S3: {e}")
        return None
    