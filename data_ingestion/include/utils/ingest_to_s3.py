import logging
from airflow.sdk import Variable

# Configure global logging

# Logs will include timestamp, log level, and message.
# INFO level is suitable for pipeline execution visibility.
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Create a module-level logger
logger = logging.getLogger(__name__)

    
def upload_data_to_s3(data, season: int, output_key: str, file_name: str="", bucket: str=Variable.get("fpl_bucket")):
    
    """
        Uploads serialized Fantasy Premier League (FPL) data to Amazon S3 as a JSON object.

        This utility function uses Airflow's S3Hook to persist data to an S3 bucket.
        The S3 object key is dynamically constructed based on the provided season
        and/or file name, allowing reuse across both seasonal and non-seasonal
        FPL datasets (e.g., fixtures history vs. static reference data).

        Behavior:
        - If ``season`` is None and ``file_name`` is provided, the data is uploaded
        using ``file_name`` as the object name.
        - Regardless of the above condition, the data is also uploaded using
        ``season`` as the object name.

        Args:
            data (Any): The data payload to be uploaded. This should be JSON-serializable.
            season (int | None): Season identifier used in the S3 object name
                (e.g., 2023). If None, the season-based upload may be skipped
                depending on usage context.
            output_key (str): S3 key prefix (folder path) under which the data
                will be stored.
            file_name (str, optional): Custom file name (without extension) to use
                when uploading non-seasonal datasets. Defaults to an empty string.
            bucket (str, optional): Name of the S3 bucket. Defaults to the value
                stored in the Airflow Variable ``fpl_bucket``.

        Returns:
            None: This function performs side effects only (uploads data to S3).
    """

    import json
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook
    from datetime import datetime
    
    try:
    
        s3 = S3Hook(aws_conn_id="fpl_aws_conn")

        if season is None and file_name != "":
            s3.load_string(
                string_data=json.dumps(data),
                key=f"{output_key}/{file_name}.json",
                bucket_name=bucket,
                replace=True
            )
            
        s3.load_string(
            string_data=json.dumps(data),
            key=f"{output_key}/{season}.json",
            bucket_name=bucket,
            replace=True
        )
        
        logger.info(f"Data successfully uploaded to s3://{bucket}/{output_key}")
        
    except Exception as e:
        logger.error(f"Failed to upload data to S3: {e}")
        return None
    