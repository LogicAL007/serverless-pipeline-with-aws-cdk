import os
from alpha_vantage.foreignexchange import ForeignExchange
from helperFunctions import write_to_s3
import json

API_KEY = os.environ["API_KEY"]
LOCATION = "s3://big-data-pipeline/datalake/forex_hourly/"

#Used for renaming coulmns
MAPPER = {
    "1. From_Currency Code": "from_currency_code",
    "2. From_Currency Name": "from_currency_name",
    "3. To_Currency Code": "to_currency_code",
    "4. To_Currency Name": "to_currency_name",
    "5. Exchange Rate": "exchange_rate",
    "6. Last Refreshed": "last_refreshed",
    "7. Time Zone": "time_zone",
    "8. Bid Price": "bid_price",
    "9. Ask Price": "ask_price",
}

FLOAT_KEYS = [
    "exchange_rate",
    "time_zone",
    "bid_price",
    "ask_price"
]

DATE_KEYS = ["last_refreshed"]

# Renames the keys in the dictionary based on the mapper
def rename_keys(data: dict, mapper: dict) -> dict:
    new_data = {}
    for old_key, new_key in mapper.items():
        new_data[new_key] = data.get(old_key)
    return new_data

# Converts specified keys to floats
def convert_float_dtypes(data: dict, keys: list) -> None:
    for key in keys:
        try:
            data[key] = float(data[key])
        except:
            data[key] = 0.0

# Creates a filename using output from alpha vantage request
def create_filename(data: dict, location: str) -> str:
    f_curr = data["from_currency_code"]
    t_curr = data["to_currency_code"]
    f_dt = data["last_refreshed"][:10]
    hour = data["last_refreshed"][11:13]
    return location + f"date={f_dt}/{f_curr}_{t_curr}_{hour}.json"

# Retrieves time series data from Alpha Vantage API and returns a pandas DataFrame
def get_currency_conversion(from_currency, to_currency) -> dict:
    cc = ForeignExchange(key=API_KEY)
    data, _ = cc.get_currency_exchange_rate(from_currency, to_currency)
    data = rename_keys(data, MAPPER)
    convert_float_dtypes(data, FLOAT_KEYS)
    return data

# Writes data in DataFrame to S3
def write_data(data, location) -> None:
    try:
        filename = create_filename(data, location)
        write_to_s3(json.dumps(data), filename)
        return {
            "statusCode": 200,
            "headers": {
                "Content-Type": "text/plain"
            },
            "body": "Request Completed"
        }
    except:
        return {
                "StatusCode": 400,
                "headers": {
                    "Content-Type": "text/plain"
            },
            "body": "Unable to write to S3"
        }

"""
sample_event = {
    "from_currency": "BTC",
    "to_currency": "USD"
}
"""
def handler(event, context):
    from_currency = event.get("from_currency")
    to_currency = event.get("to_currency")
    if not from_currency and to_currency:
        return {
                "StatusCode": 400,
                "headers": {
                    "Content-Type": "text/plain"
            },
            "body": "Request must include from_currency and to_currency"
        }
    data = get_currency_conversion(from_currency, to_currency)
    status = write_data(data, LOCATION)
    return status