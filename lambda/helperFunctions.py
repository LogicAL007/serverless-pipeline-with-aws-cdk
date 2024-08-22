import boto3
import json
import io
import gzip
import pyarrow.parquet as pq

# This file contains various functions to help facilitate
# reading and writing data to/from S3

def get_object_resource(filename=None, bucket=None, key=None):
    assert filename or (bucket and key), "Args must include either filename or bucket and key"
    if filename:
        bucket, key = parse_s3_filename(filename)
    s3 = boto3.resource("s3")
    return s3.Object(bucket, key)

def parse_s3_filename(filename: str):
    bucket_name = filename.split("/")[2]
    key="/".join(filename.split("/")[3:])
    return bucket_name, key

def read_s3_file(filename=None, bucket=None, key=None):
    s3_obj = get_object_resource(filename, bucket, key)
    try:
        return s3_obj.get()["Body"].read() 
    except:
        raise

def write_to_s3(data, filename=None, bucket=None, key=None) -> None:
    s3_obj = get_object_resource(filename, bucket, key)
    try:
     s3_obj.put(Body=data)
    except:
        raise

def s3_gzip_to_json(filename=None, bucket=None, key=None):
    data = read_s3_file(filename, bucket, key)
    gzip_io = io.BytesIO(data)
    gzip_data = gzip.GzipFile(fileobj=gzip_io)
    return json.loads(gzip_data.read())

def write_parquet_table_to_s3(table, filename=None, bucket=None, key=None):
    bytes_io = io.BytesIO()
    pq.write_table(table, bytes_io, compression = "snappy")
    write_to_s3(bytes_io.getvalue(), filename, bucket, key)