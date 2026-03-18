import time
from pathlib import Path

import pandas as pd
import yfinance as yf

from utils.decorators import log_execution
from utils.logger import get_logger

logger = get_logger(__name__)


@log_execution
def process_stock(symbol: str) -> None:
    yf_symbol = f"{symbol}.NS"
    file_path = Path("data/raw") / f"{symbol}.csv"

    logger.info("Fetching historical data for %s", symbol)

    ticker = yf.Ticker(yf_symbol)
    df = ticker.history(period="2y", auto_adjust=False)

    if df.empty:
        logger.warning("No data found for %s", symbol)
        return

    df = df.reset_index()
    df["Date"] = pd.to_datetime(df["Date"]).dt.tz_localize(None)
    df["Symbol"] = symbol

    df["Daily Return (%)"] = df["Adj Close"].pct_change() * 100
    df["Intraday Volatility (%)"] = ((df["High"] - df["Low"]) / df["Open"]) * 100
    df["Overnight Gap (%)"] = (
        (df["Open"] - df["Close"].shift(1)) / df["Close"].shift(1)
    ) * 100
    df["Typical Price"] = (df["High"] + df["Low"] + df["Close"]) / 3
    df["Estimated Turnover"] = df["Typical Price"] * df["Volume"]

    df["Prev Close"] = df["Close"].shift(1)
    df["TR1"] = df["High"] - df["Low"]
    df["TR2"] = abs(df["High"] - df["Prev Close"])
    df["TR3"] = abs(df["Low"] - df["Prev Close"])
    df["True Range"] = df[["TR1", "TR2", "TR3"]].max(axis=1)

    df["20-Day SMA"] = df["Close"].rolling(window=20).mean()
    df["50-Day SMA"] = df["Close"].rolling(window=50).mean()
    df["200-Day SMA"] = df["Close"].rolling(window=200).mean()

    delta = df["Close"].diff()
    gain = delta.where(delta > 0, 0).rolling(window=14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
    rs = gain / loss
    df["14-Day RSI"] = 100 - (100 / (1 + rs))

    columns = [
        "Date",
        "Symbol",
        "Open",
        "High",
        "Low",
        "Close",
        "Adj Close",
        "Volume",
        "Dividends",
        "Stock Splits",
        "Daily Return (%)",
        "Intraday Volatility (%)",
        "Overnight Gap (%)",
        "Typical Price",
        "Estimated Turnover",
        "True Range",
        "20-Day SMA",
        "50-Day SMA",
        "200-Day SMA",
        "14-Day RSI",
    ]

    df_final = df[columns].copy()

    file_path.parent.mkdir(parents=True, exist_ok=True)

    if not file_path.exists():
        df_final.to_csv(file_path, index=False)
        logger.info("Created %s with %d rows", file_path, len(df_final))
    else:
        today_row = df_final.tail(1)
        today_row.to_csv(file_path, mode="a", header=False, index=False)
        logger.info("Appended latest row to %s", file_path)


def main():
    list_path = Path("data/stocks_list.csv")
    
    if not list_path.exists():
        logger.error("Stock list not found at %s", list_path)
        return

    df_symbols = pd.read_csv(list_path)
    symbols = df_symbols["Symbol"].tolist()

    logger.info("Starting pricing fetch for %d stocks...", len(symbols))

    for i, symbol in enumerate(symbols, 1):
        logger.info("Processing Pricing %d/%d: %s", i, len(symbols), symbol)
        try:
            process_stock(symbol)
            time.sleep(1) 
        except Exception as e:
            logger.error("Failed to process %s: %s", symbol, e)


if __name__ == "__main__":
    main()