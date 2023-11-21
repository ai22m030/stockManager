import requests
from pymongo import MongoClient
from datetime import datetime, timedelta

# MongoDB setup
client = MongoClient('localhost', 27017)
db = client.HistoricalStocks
collection = db.StockPrices

# API details
api_key = 'T5CHYLWGF8OCKHU5'  # Replace with your actual Alpha Vantage API key
base_url = "https://www.alphavantage.co/query"

# Stock symbols
symbols = [
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


def fetch_and_store_data(symbol, month):
    params = {
        "function": "TIME_SERIES_INTRADAY",
        "symbol": symbol,
        "interval": "60min",
        "month": month,
        "outputsize": "full",
        "apikey": api_key
    }

    response = requests.get(base_url, params=params)
    if response.status_code == 200:
        data = response.json()
        # Assuming 'Time Series (60min)' is the key in the JSON response
        time_series = data.get("Time Series (60min)", {})
        for date, stock_data in time_series.items():
            stock_data['symbol'] = symbol
            stock_data['date'] = date.to_pydatetime()
            collection.insert_one(stock_data)
    else:
        print(f"Error fetching data for {symbol} for the month {month}")


if __name__ == '__main__':
    start_date = datetime(2000, 1, 1)
    end_date = datetime.now()
    current_date = start_date

    while current_date <= end_date:
        month = current_date.strftime('%Y-%m')
        for symbol in symbols:
            fetch_and_store_data(symbol, month)
        current_date += timedelta(days=31)
