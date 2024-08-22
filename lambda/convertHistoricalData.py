import os
from datetime import datetime
import pandas as pd
import pyarrow as pa
from helperFunctions import s3_gzip_to_json, write_parquet_table_to_s3

DEST_PREFIX = "datalake/forex_historical/"
BUCKET = os.environ["BUCKET_NAME"]

# Defining the schema here will ensure that there are no
# problems when creating accessing the data through Glue
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

# Function used to parse the data in the s3 files
def gzip_json_to_pandas(filename):
    data = s3_gzip_to_json(filename=filename)
    rows = []
    for key1, val1 in data.items():
        for key2, val2 in val1.items():
            for key3, val3 in val2.items():
                row = {
                    "from_currency": key1,
                    "to_currency": key2,
                    "date": datetime.strptime(key3, "%Y-%m-%d")
                }
                row.update(val3)
                rows.append(row)
        return pd.DataFrame(rows)

# File name example "s3://big-data-pipeline/data/forex_historical/202210_forex.json.gz" 
def handler(event, context):
    if not (event.get("Records") or event.get("FileSource")):
        return {
            "statusCode": 400,
            "headers": {
                "Content-Type": "text/plain"
            },
            "body": "Event must include FileSource and FileDest"
        }
    if event.get("Records"):
        bucket = event["Records"][0]["s3"]["bucket"]["name"]
        key = event["Records"][0]["s3"]["object"]["key"]
        key_name = key.split("/")[-1]
        file_source = f"s3://{bucket}/{key}"
        file_name = key_name.replace(".json.gz",".parquet")
        file_dest = f"s3://{bucket}/{DEST_PREFIX}{file_name}"
    else:
        file_source = event.get("FileSource")
        file_dest = event.get("FileDest")
    df = gzip_json_to_pandas(file_source)
    table = pa.Table.from_pandas(df, FILE_SCHEMA)
    write_parquet_table_to_s3(table, filename=file_dest)
    return {
        "statusCode": 200,
        "headers": {
            "Content-Type": "text/plain"
        },
        "body": "Request Submitted"
    }

