import boto3
import json

TICKERS = ["MSFT", "AMZN", "IBM"]
CRAWLERS = ["intraday_stock_data_crawler", "forex_hourly_crawler"]

def invoke_intraday_lambda(ticker: str) -> dict:
    payload = {"ticker": ticker, "backfill": True}
    client = boto3.client("lambda")
    response = client.invoke(
        FunctionName="intraday_data_handler",
        InvocationType="RequestResponse",
        Payload=json.dumps(payload)
    )
    return response

def start_rds_glue_job(job_name: str) -> dict:
    client = boto3.client("glue")
    response = client.start_job_run(
        JobName=job_name,
        WorkerType="G.1X",
        NumberOfWorkers=10
    )
    return response

def run_crawler(crawler_name: str) -> dict:
    client = boto3.client("glue")
    response = client.start_crawler(
        Name=crawler_name
    )
    return response

if __name__ == "__main__":
    ########################################################################################
    # Backfill historical data from Postgres to S3
    response = start_rds_glue_job("rds_extract_job")
    ########################################################################################


    ########################################################################################
    # Backfill intraday stock data
    for ticker in TICKERS:
        response = invoke_intraday_lambda(ticker)
        print(response)
    ########################################################################################


    ########################################################################################
    # Run Crawlers to update tables
    for crawler in CRAWLERS:
        response = run_crawler(crawler)
        print(response)
    ########################################################################################