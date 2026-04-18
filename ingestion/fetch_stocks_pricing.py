import time
from datetime import datetime

import pandas as pd
import pytz
import yfinance as yf

from ingestion.fetch_stocks_list import fetch_stock_list
from utils.decorators import log_execution
from utils.logger import get_logger
from utils.s3_helper import write_parquet_to_s3

logger = get_logger(__name__)


def process_stock(symbol: str) -> pd.DataFrame | None:
    """
    Fetch raw OHLCV data for a single stock from yfinance.
    Returns only today's row — no feature engineering.
    Feature engineering happens in Silver layer.
    """
    try:
        ticker = yf.Ticker(f"{symbol}.NS")

        # Fetch 5 days to ensure we catch today
        # (yfinance sometimes delays by 1 day)
        df = ticker.history(period="5d", auto_adjust=False)

        if df.empty:
            logger.warning("No data returned for %s, skipping.", symbol)
            return None

        df = df.reset_index()
        df["Date"] = pd.to_datetime(df["Date"]).dt.tz_localize(None)
        df["Symbol"] = symbol

        # Keep only raw columns — no calculations here
        raw_columns = [
            "Date",
            "Symbol",
            "Open",
            "High",
            "Low",
            "Close",
            "Adj Close",
            "Volume",
        ]

        df = df[raw_columns].copy()

        # Return only today's row
        ist = pytz.timezone("Asia/Kolkata")
        today = datetime.now(ist).date()
        df_today = df[df["Date"].dt.date == today]

        if df_today.empty:
            logger.warning("No data for today for %s, skipping.", symbol)
            return None

        return df_today

    except Exception as e:
        logger.error("Failed to process %s: %s", symbol, e)
        return None


@log_execution
def main():
    symbols = fetch_stock_list()

    if not symbols:
        logger.error("Failed to retrieve stock list. Aborting.")
        return

    total = len(symbols)
    all_dfs = []
    success_count = 0
    fail_count = 0
    failed_symbols = []

    logger.info("Starting bronze ingestion for %d stocks...", total)

    for i, symbol in enumerate(symbols, 1):
        df = process_stock(symbol)

        if df is not None:
            all_dfs.append(df)
            success_count += 1
        else:
            fail_count += 1
            failed_symbols.append(symbol)

        logger.info(
            "[%d/%d] %s | Success: %d | Failed: %d | Remaining: %d",
            i, total, symbol, success_count, fail_count, total - i
        )

        time.sleep(1)  # avoid yfinance rate limiting

    # Nothing collected today — market holiday or all failed
    if not all_dfs:
        logger.error("No data collected today. Nothing written to S3.")
        return

    # Combine all stocks into one DataFrame
    combined_df = pd.concat(all_dfs, ignore_index=True)

    # Build today's S3 partition path
    ist = pytz.timezone("Asia/Kolkata")
    today = datetime.now(ist).date()
    s3_key = (
        f"bronze/stocks/"
        f"year={today.year}/"
        f"month={today.month:02d}/"
        f"day={today.day:02d}/"
        f"data.parquet"
    )

    write_parquet_to_s3(combined_df, s3_key)

    logger.info(
        "Bronze layer complete | Total: %d | Success: %d | Failed: %d",
        total, success_count, fail_count
    )

    if failed_symbols:
        logger.warning("Failed symbols (%d): %s", len(failed_symbols), failed_symbols)


if __name__ == "__main__":
    main()