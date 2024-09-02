import os
from alpha_vantage.timeseries import TimeSeries
import pandas as pd
from datetime import datetime, timedelta, date
import pyarrow as pa
from helperFunctions import write_parquet_table_to_s3

API_KEY = os.environ["API_KEY"]
LOCATION = "s3://big-data-pipeline/datalake/stock_data_intraday/"

# Column names mapping
COLUMN_MAPPER = {
    "date": "datetime",
    "1. open": "open",
    "2. high": "high",
    "3. low": "low",
    "4. close": "close",
    "5. volume": "volume"
}

COLUMN_ORDER = [
    "datetime",
    "ticker",
    "open",
    "high",
    "low",
    "close",
    "volume"
]

# Schema for the Parquet file
FILE_SCHEMA = pa.schema([
    ("datetime", pa.timestamp("s")),
    ("ticker", pa.string()),
    ("open", pa.float64()),
    ("high", pa.float64()),
    ("low", pa.float64()),
    ("close", pa.float64()),
    ("volume", pa.float64())
])

def get_stock_data(ticker: str) -> pd.DataFrame:
    """Fetches intraday stock data from Alpha Vantage API."""
    ts = TimeSeries(key=API_KEY, output_format="pandas")
    data, _ = ts.get_intraday(symbol=ticker, interval="15min", outputsize="full")
    data.reset_index(inplace=True)
    data["ticker"] = ticker
    data.rename(columns=COLUMN_MAPPER, inplace=True)
    return data[COLUMN_ORDER]

def write_daily_data(df: pd.DataFrame, specific_dates: list) -> None:
    """Writes daily stock data to S3 in Parquet format."""
    ticker = df["ticker"].unique()[0]
    dates_to_process = specific_dates or df["datetime"].dt.date.unique()

    for single_date in dates_to_process:
        date_str = single_date.strftime("%Y-%m-%d")
        file_location = f"{LOCATION}date={date_str}/{ticker}.parquet"
        day_data = df[df["datetime"].dt.date == single_date]
        table = pa.Table.from_pandas(day_data, schema=FILE_SCHEMA)
        write_parquet_table_to_s3(table, uri=file_location)

def lambda_handler(event, context):
    """Handles Lambda event for processing stock data."""
    try:
        dates = [datetime.strptime(d, "%Y-%m-%d") for d in event.get("dates", [])]
    except ValueError:
        return {
            "statusCode": 400,
            "headers": {"Content-Type": "text/plain"},
            "body": "Invalid Dates! Dates must be in format 'YYYY-MM-DD'"
        }

    ticker = event.get("ticker")
    if not ticker:
        return {
            "statusCode": 400,
            "headers": {"Content-Type": "text/plain"},
            "body": "Request Failed! No ticker included in request"
        }

    # Determine dates to process
    if not event.get("backfill"):
        dates = dates or [date.today() - timedelta(days=1)]

    data = get_stock_data(ticker)
    write_daily_data(data, dates)

    return {
        "statusCode": 200,
        "headers": {"Content-Type": "text/plain"},
        "body": "Request Completed"
    }
