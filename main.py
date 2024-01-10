import requests
import time
from pymongo import MongoClient, UpdateOne
from datetime import datetime, timedelta
from urllib.parse import quote_plus
import configparser
import calendar


config = configparser.ConfigParser()
config.read('config.ini')

# MongoDB setup
uri = "mongodb://%s:%s@%s:%s/%s" % (quote_plus(config['MongoDB']['user']),
                                    quote_plus(config['MongoDB']['password']),
                                    config['MongoDB']['host'],
                                    config['MongoDB']['port'],
                                    config['MongoDB']['database'])
client = MongoClient(uri)
db = client.StockData
collection = db.StockPrices
collection.create_index([("symbol", 1), ("date", 1)], unique=True)

# API details
api_key = config['API']['key']
base_url = config['API']['host']

# Stock symbols
symbols_stocks = [
    'AAPL', 'MSFT', 'CSCO', 'WMT', 'INTC', 'PG', 'JNJ', 'KO', 'PEP', 'MCD',
    'MO', 'BA', 'XOM', 'CVX', 'GE', 'CAT', 'MMM', 'HPQ', 'DD', 'MRK',
    'JPM', 'AXP', 'BAC', 'C', 'WFC', 'GS', 'VZ', 'T', 'IBM', 'TXN',
    'HON', 'LMT', 'GD', 'NOC', 'GM', 'F', 'PFE', 'ABT', 'BMY', 'LLY',
    'AIG', 'MET', 'HIG', 'ALL', 'TRV', 'PGR', 'CINF', 'BRK-A', 'DIS', 'TGT',
    'CMCSA', 'NWSA', 'AMCX', 'MDLZ', 'CPB', 'K', 'GIS', 'HSY',
    'HRL', 'SJM', 'MKC', 'FLO', 'ADM', 'BG', 'INGR', 'TSN', 'HOG', 'HD',
    'LOW', 'WHR', 'NWL', 'LEG', 'MHK', 'MAS', 'PHM', 'DHI', 'LEN', 'KBH',
    'BZH', 'ORCL', 'GLW', 'EMR', 'ETN', 'SLB', 'HAL', 'DE', 'CL', 'EOG',
    'OXY', 'DVN', 'MRO', 'SWN', 'RRC', 'CVS', 'EQT', 'FTI', 'AMZN', 'GOOGL',
    'FB', 'NFLX', 'TSLA', 'AMD', 'NVDA', 'SBUX', 'NKE', 'ADI', 'QCOM', 'MS',
    'BLK', 'CME', 'COF', 'PYPL', 'MA', 'V', 'ADBE', 'CRM', 'ORLY', 'WBA',
    'KR', 'GILD', 'AMGN', 'UNH', 'ANTM', 'CI', 'AET', 'HUM', 'BKNG'
]

# Rate limitations
request_count = 0
minute_start = time.time()


def handle_rate_limit():
    global request_count, minute_start
    if time.time() - minute_start > 60:
        request_count = 0
        minute_start = time.time()

    if request_count >= 30:
        sleep_time = 60 - (time.time() - minute_start)
        print(f"Rate limit reached, sleeping for {sleep_time} seconds")
        time.sleep(sleep_time)
        request_count = 0
        minute_start = time.time()


def fetch_and_store_data(symbol, month, interval="1min"):
    global request_count
    handle_rate_limit()

    if collection.count_documents({"symbol": symbol, "date": {"$gte": datetime.strptime(month, '%Y-%m'),
                                                              "$lt": datetime.strptime(month, '%Y-%m') + timedelta(
                                                                      days=31)}}):
        print(f"Data for {symbol} for the month {month} already exists. Skipping API call.")
        return

    params = {
        "function": "TIME_SERIES_INTRADAY",
        "symbol": symbol,
        "interval": interval,
        "month": month,
        "outputsize": "full",
        "apikey": api_key
    }

    response = requests.get(base_url, params=params)
    request_count += 1

    if response.status_code == 200:
        data = response.json()
        if "Information" in data:
            print(f"Error fetching data for {symbol} for the month {month}")
            return

        time_series = data.get(f"Time Series ({interval})", {})

        # Prepare bulk update operations
        operations = []
        for date, stock_data in time_series.items():
            operation = UpdateOne(
                {"symbol": symbol, "date": datetime.strptime(date, '%Y-%m-%d %H:%M:%S')},
                {"$set": parse_data(stock_data, symbol, date)},
                upsert=True
            )
            operations.append(operation)

        # Execute bulk write
        if operations:
            collection.bulk_write(operations)
    else:
        print(f"Error fetching data for {symbol} for the month {month}")


def parse_data(data, symbol, date):
    return {
        "open": float(data['1. open']),
        "high": float(data['2. high']),
        "low": float(data['3. low']),
        "close": float(data['4. close']),
        "volume": int(data['5. volume']),
        "symbol": symbol,
        "date": datetime.strptime(date, '%Y-%m-%d %H:%M:%S')
    }


def add_one_month(orig_date):
    new_year = orig_date.year
    new_month = orig_date.month + 1
    if new_month > 12:
        new_month = 1
        new_year += 1
    last_day = calendar.monthrange(new_year, new_month)[1]
    return datetime(new_year, new_month, min(orig_date.day, last_day))


if __name__ == '__main__':
    start_date = datetime(2005, 1, 1)
    end_date = datetime.now()
    current_date = start_date

    while current_date <= end_date:
        month = current_date.strftime('%Y-%m')

        print(f"Fetching data for the month {month}")

        for symbol in symbols_stocks:
            print(f"Fetching data for {symbol}")
            fetch_and_store_data(symbol, month)

        # Add a month to the current date
        current_date = add_one_month(current_date)
