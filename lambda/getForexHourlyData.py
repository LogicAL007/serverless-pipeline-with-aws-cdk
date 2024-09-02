import os
from datetime import datetime
import pandas as pd
import pyarrow as pa
from helperFunctions import s3_gzip_to_json, write_parquet_table_to_s3

BUCKET = os.environ["BUCKET_NAME"]
DEST_PREFIX = "datalake/forex_historical/"

# Schema definition for Parquet files to ensure compatibility with AWS Glue
FILE_SCHEMA = pa.schema([
    ("from_currency", pa.string()),
    ("to_currency", pa.string()),
    ("date", pa.date64()),
    ("open", pa.float64()),
    ("high", pa.float64()),
    ("low", pa.float64()),
    ("close", pa.float64()),
    ("adj_close", pa.float64()),
    ("volume", pa.float64())
])

def convert_gzip_json_to_dataframe(file_path: str) -> pd.DataFrame:
    """Convert gzipped JSON data from S3 to a DataFrame."""
    data = s3_gzip_to_json(filename=file_path)
    rows = [
        {
            "from_currency": currency_pair.split("_")[0],
            "to_currency": currency_pair.split("_")[1],
            "date": datetime.strptime(date, "%Y-%m-%d"),
            **values
        }
        for currency_pair, dates in data.items()
        for date, values in dates.items()
    ]
    return pd.DataFrame(rows)

def process_file(event: dict):
    """Process a file based on Lambda event trigger."""
    file_source = file_dest = ""
    if "Records" in event:
        record = event["Records"][0]
        bucket = record["s3"]["bucket"]["name"]
        key = record["s3"]["object"]["key"]
        file_source = f"s3://{bucket}/{key}"
        file_name = key.split("/")[-1].replace(".json.gz", ".parquet")
        file_dest = f"s3://{bucket}/{DEST_PREFIX}{file_name}"
    elif "FileSource" in event and "FileDest" in event:
        file_source = event["FileSource"]
        file_dest = event["FileDest"]

    if not file_source or not file_dest:
        return {
            "statusCode": 400,
            "body": "Missing file source or destination in event."
        }

    df = convert_gzip_json_to_dataframe(file_source)
    table = pa.Table.from_pandas(df, schema=FILE_SCHEMA)
    write_parquet_table_to_s3(table, filename=file_dest)
    return {
        "statusCode": 200,
        "body": "Data successfully processed and saved."
    }

def lambda_handler(event, context):
    """Lambda function to handle S3 event for processing JSON to Parquet."""
    response = process_file(event)
    return {
        "statusCode": response.get("statusCode", 500),
        "headers": {
            "Content-Type": "text/plain"
        },
        "body": response.get("body", "An unexpected error occurred.")
    }
