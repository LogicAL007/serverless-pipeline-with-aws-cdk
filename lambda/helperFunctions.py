import boto3
import json
import io
import gzip
import pyarrow.parquet as pq

# This module contains functions to facilitate
# reading from and writing to S3.

def get_s3_resource():
    """Get the S3 resource."""
    return boto3.resource("s3")

def parse_s3_uri(uri: str):
    """Extract bucket and key from S3 URI."""
    assert uri.startswith("s3://"), "URI must start with 's3://'"
    without_scheme = uri.split("//")[1]
    bucket, key = without_scheme.split('/', 1)
    return bucket, key

def get_s3_object(bucket: str, key: str):
    """Retrieve an object from S3."""
    s3 = get_s3_resource()
    return s3.Object(bucket, key)

def read_s3_file(uri: str):
    """Read data from an S3 file."""
    bucket, key = parse_s3_uri(uri)
    s3_obj = get_s3_object(bucket, key)
    try:
        return s3_obj.get()["Body"].read()
    except boto3.exceptions.Boto3Error as e:
        raise RuntimeError(f"Failed to read from {uri}") from e

def write_to_s3(data, uri: str) -> None:
    """Write data to an S3 file."""
    bucket, key = parse_s3_uri(uri)
    s3_obj = get_s3_object(bucket, key)
    try:
        s3_obj.put(Body=data)
    except boto3.exceptions.Boto3Error as e:
        raise RuntimeError(f"Failed to write to {uri}") from e

def s3_gzip_to_json(uri: str):
    """Read a gzipped JSON file from S3 and return its contents."""
    compressed_data = read_s3_file(uri)
    with gzip.GzipFile(fileobj=io.BytesIO(compressed_data)) as gzip_file:
        return json.load(gzip_file)

def write_parquet_table_to_s3(table, uri: str):
    """Write a PyArrow Table to S3 as a Parquet file."""
    with io.BytesIO() as buffer:
        pq.write_table(table, buffer, compression="snappy")
        write_to_s3(buffer.getvalue(), uri)
