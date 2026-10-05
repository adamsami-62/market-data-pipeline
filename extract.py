"""
extract.py — Pull daily stock prices from the Alpha Vantage API and
save each ticker's raw response to a dated JSON file.
"""

import requests, json, os, time, datetime
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.environ["ALPHAVANTAGE_API_KEY"]
TICKERS = ["AAPL", "MSFT", "GOOGL", "AMZN", "JPM"]


def extract_one(ticker):
    """Fetch the daily price history for a single ticker.

    Returns the raw API response as a Python dict. Raises if the request
    fails or if the API returns a non-data response.
    """
    url = "https://www.alphavantage.co/query"
    params = {
        "function": "TIME_SERIES_DAILY",
        "symbol": ticker,
        "outputsize": "compact",
        "apikey": API_KEY,
    }
    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()

    data = resp.json()
    if "Time Series (Daily)" not in data:   # API sends a note (not an error) when rate-limited
        raise ValueError(f"No price data for {ticker}: {data}")
    return data


def extract(run_date):
    """Fetch every ticker and save each raw response to a dated file in data/raw/."""
    os.makedirs("data/raw", exist_ok=True)
    for ticker in TICKERS:
        data = extract_one(ticker)
        path = f"data/raw/{ticker}_{run_date}.json"
        with open(path, "w") as f:
            json.dump(data, f)
        print("wrote", path)
        time.sleep(15)


if __name__ == "__main__":
    extract(datetime.date.today().isoformat())