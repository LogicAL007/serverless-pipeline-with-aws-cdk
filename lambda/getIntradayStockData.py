import os
from alpha_vantage.timeseries import TimeSeries
import pandas as pd
from datetime import datetime, timedelta, date
import pyarrow as pa
from helperFunctions import write_parquet_table_to_s3

API_KEY = os.environ["API_KEY"]
LOCATION = "s3://big-data-pipeline/datalake/stock_data_intraday/"

#Used for renaming coulmns
MAPPER = {
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

# Parquet file schema
FILE_SCHEMA = pa.schema([
    ("datetime", pa.timestamp("s")),
    ("ticker", pa.string()),
    ("open", pa.float64()),
    ("high", pa.float64()),
    ("low", pa.float64()),
    ("close", pa.float64()),
    ("volume", pa.float64())
])

# Retrieves time series data from Alpha Vantage API and returns a pandas DataFrame
def get_data(ticker) -> pd.DataFrame:
    ts = TimeSeries(key=API_KEY,output_format="pandas")
    df, meta_data = ts.get_intraday(symbol=ticker,interval="15min", outputsize="full")
    df.reset_index(inplace = True)
    df["ticker"] = ticker
    df.rename(MAPPER, inplace = True, axis = 1)
    return df[COLUMN_ORDER]

# Writes data in DataFrame to S3
def write_data(df, dates) -> None:
    date_filter = []
    ticker = df["ticker"].unique()[0]
    # Date filter includes all dates returned if the dates object is empty
    if len(dates) == 0:
        date_filter += list(df["datetime"].dt.date.unique())
    else:
        date_filter += dates
    # Write a file for each date in the date filter
    for date in date_filter:
        date_string = date.strftime("%Y-%m-%d")
        location = f"{LOCATION}date={date_string}/{ticker}.parquet"
        filtered_df = df[df["datetime"].dt.date == date]
        table = pa.Table.from_pandas(filtered_df, FILE_SCHEMA)
        write_parquet_table_to_s3(table, filename=location)

"""
sample_event = {
    "ticker": "MSFT",
    "backfill": true|false
    "dates": ["2022-11-08"]
}
"""
def handler(event, context):
    dates = []
    # If the the invocation includes a date filter and not a backfill flag
    # create a list of dates from the list of date strings
    if event.get("dates") and not event.get("backfill"):
        try:
            dates = [datetime.strptime(x,"%Y-%m-%d") for x in event.get("dates")]
        except Exception as e:
            return {
                "StatusCode": 400,
                "headers": {
                    "Content-Type": "text/plain"
            },
            "body": "Invalid Dates! Dates must be in format 'YYYY-MM-DD'"
        }
    # If there is no backfill flag and and no date filter, 
    # the handler will be run for yesterday
    if not event.get("backfill") and not event.get("dates"):
        dates.append(date.today() - timedelta(days=1))
    if not event.get("ticker"):
        return {
            "StatusCode": 400,
            "headers": {
                "Content-Type": "text/plain"
        },
        "body": "Request Failed! No ticker included in request"
    }
    # Although this isn"t necessary it provides ease of readability
    # If there is a backfill flag, the date filter is left empty which will 
    # backfill the the data for the given ticker
    if event.get("backfill"):
        dates = []
    df = get_data(event.get("ticker"))
    write_data(df, dates)
    return {
        "statusCode": 200,
        "headers": {
            "Content-Type": "text/plain"
        },
        "body": "Request Completed"
    }