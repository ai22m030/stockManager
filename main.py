import requests
from pymongo import MongoClient
from datetime import datetime, timedelta
from urllib.parse import quote_plus
import configparser

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
    'OXY', 'DVN', 'MRO', 'SWN', 'RRC', 'CVS', 'EQT', 'FTI'
]

symbols_currency = ["EUR", "GBP", "JPY", "CHF", "CAD"]


def fetch_and_store_data(symbol, month, interval="1min", fetch_currency=False):
    # Check if data for this symbol and month already exists in the database
    if fetch_currency:
        from_to = symbol.split("-")

    if collection.count_documents({"symbol": symbol, "date": {"$gte": datetime.strptime(month, '%Y-%m'),
                                                              "$lt": datetime.strptime(month, '%Y-%m') + timedelta(
                                                                      days=31)}}):
        print(f"Data for {symbol} for the month {month} already exists. Skipping API call.")
        return

    if fetch_currency:
        params = {
            "function": "CURRENCY_EXCHANGE_RATE",
            "from_symbol": from_to[0],
            "to_symbol": from_to[1],
            "interval": interval,  # Assuming you want 60min interval data
            "outputsize": "full",
            "apikey": api_key
        }
    else:
        params = {
            "function": "TIME_SERIES_INTRADAY",
            "symbol": symbol,
            "interval": interval,  # Assuming you want 60min interval data
            "month": month,
            "outputsize": "full",
            "apikey": api_key
        }

    response = requests.get(base_url, params=params)

    if response.status_code == 200:
        data = response.json()

        if "Information" in data:
            print(f"Error fetching data for {symbol} for the month {month}")
            return

        time_series = data.get(f"Time Series ({interval})", {})
        for date, stock_data in time_series.items():
            tmp_data = dict()
            tmp_data['open'] = float(stock_data['1. open'])
            tmp_data['high'] = float(stock_data['2. high'])
            tmp_data['low'] = float(stock_data['3. low'])
            tmp_data['close'] = float(stock_data['4. close'])
            tmp_data['volume'] = int(stock_data['5. volume'])
            tmp_data['symbol'] = symbol
            tmp_data['date'] = datetime.strptime(date, '%Y-%m-%d %H:%M:%S')
            collection.update_one(
                {"symbol": symbol, "date": tmp_data['date']},
                {"$set": tmp_data},
                upsert=True
            )
    else:
        print(f"Error fetching data for {symbol} for the month {month}")


if __name__ == '__main__':
    start_date = datetime(2020, 1, 1)
    end_date = datetime.now()
    current_date = start_date

    while current_date <= end_date:
        month = current_date.strftime('%Y-%m')

        print(f"Fetching data for the month {month}")

        for symbol in symbols_stocks:
            print(f"Fetching data for {symbol}")
            fetch_and_store_data(symbol, month)

        # Add a month to the current date
        current_date += timedelta(days=31)
