import time
from pathlib import Path

import pandas as pd
import yfinance as yf

from ingestion.fetch_stocks_list import fetch_stock_list
from utils.decorators import log_execution
from utils.logger import get_logger

logger = get_logger(__name__)


@log_execution
def process_stock(symbol: str) -> None:
    yf_symbol = f"{symbol}.NS"
    file_path = Path("data/raw") / f"{symbol}.csv"

    ticker = yf.Ticker(yf_symbol)
    df = ticker.history(period="2y", auto_adjust=False)

    if df.empty:
        raise ValueError(f"No data found for {symbol}")

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
    else:
        try:
            existing_dates = pd.read_csv(file_path, usecols=["Date"])
            
            if not existing_dates.empty:
                last_saved_date = pd.to_datetime(existing_dates["Date"].iloc[-1])
                new_rows = df_final[df_final["Date"] > last_saved_date]
                
                if not new_rows.empty:
                    new_rows.to_csv(file_path, mode="a", header=False, index=False)
            else:
                df_final.to_csv(file_path, index=False)
                
        except Exception as e:
            logger.warning("Could not read existing file for %s (%s). Overwriting.", symbol, e)
            df_final.to_csv(file_path, index=False)


def main():
    symbols = fetch_stock_list()

    if not symbols:
        logger.error("Failed to retrieve live stock list. Aborting pricing fetch.")
        return

    total_symbols = len(symbols)
    success_count = 0
    fail_count = 0
    failed_symbols = []

    logger.info("Starting pricing fetch for %d stocks...", total_symbols)

    for i, symbol in enumerate(symbols, 1):
        remaining = total_symbols - i

        try:
            process_stock(symbol)
            success_count += 1
            logger.info(
                "[%d/%d] Processed %s | Success: %d | Failed: %d | Remaining: %d",
                i, total_symbols, symbol, success_count, fail_count, remaining
            )
            time.sleep(1)
        except Exception as e:
            fail_count += 1
            failed_symbols.append(symbol)
            logger.error(
                "[%d/%d] Failed %s: %s | Success: %d | Failed: %d | Remaining: %d",
                i, total_symbols, symbol, e, success_count, fail_count, remaining
            )

    logger.info(
        "Fetch Complete! Total: %d | Success: %d | Failed: %d",
        total_symbols, success_count, fail_count
    )

    if failed_symbols:
        logger.warning(
            "Failed pricing symbols (%d): %s",
            len(failed_symbols),
            failed_symbols
        )
    else:
        logger.info("No pricing ingestion failures detected.")


if __name__ == "__main__":
    main()