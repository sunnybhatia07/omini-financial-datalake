import time
from datetime import datetime

import pandas as pd
import pytz
import yfinance as yf

from ingestion.fetch_stocks_list import fetch_stock_list
from utils.logger import get_logger
from utils.s3_helper import write_parquet_to_s3, list_s3_keys

logger = get_logger(__name__)


def get_existing_partitions() -> set:
    """
    Get all date partitions already written to S3.
    Returns a set of strings like {'2025-04-14', '2025-04-13'}
    So we never overwrite already completed days.
    """
    keys = list_s3_keys("bronze/stocks/")
    existing = set()

    for key in keys:
        # Extract date from key like:
        # bronze/stocks/year=2025/month=04/day=14/data.parquet
        try:
            parts = key.split("/")
            year  = parts[2].split("=")[1]
            month = parts[3].split("=")[1]
            day   = parts[4].split("=")[1]
            existing.add(f"{year}-{month}-{day}")
        except Exception:
            continue

    logger.info("Found %d existing partitions in S3", len(existing))
    return existing


def fetch_full_history(symbol: str) -> pd.DataFrame | None:
    """
    Fetch maximum available history for a single stock.
    Returns raw OHLCV DataFrame — no feature engineering.
    """
    try:
        ticker = yf.Ticker(f"{symbol}.NS")
        df = ticker.history(period="5y", auto_adjust=False)

        if df.empty:
            logger.warning("No data for %s, skipping.", symbol)
            return None

        df = df.reset_index()
        df["Date"] = pd.to_datetime(df["Date"]).dt.tz_localize(None)
        df["Symbol"] = symbol

        raw_columns = [
            "Date", "Symbol",
            "Open", "High", "Low", "Close", "Adj Close",
            "Volume", "Dividends", "Stock Splits",
        ]

        return df[raw_columns].copy()

    except Exception as e:
        logger.error("Failed to fetch %s: %s", symbol, e)
        return None


def main():
    symbols = fetch_stock_list()

    if not symbols:
        logger.error("No symbols retrieved. Aborting backfill.")
        return

    total = len(symbols)
    logger.info("Starting backfill for %d stocks...", total)

    # Step 1 — fetch all stock histories
    all_dfs = []
    for i, symbol in enumerate(symbols, 1):
        df = fetch_full_history(symbol)
        if df is not None:
            all_dfs.append(df)
        logger.info("[%d/%d] Fetched %s", i, total, symbol)
        time.sleep(1)  # avoid rate limiting

    if not all_dfs:
        logger.error("No data fetched. Aborting.")
        return

    # Step 2 — combine all stocks into one big DataFrame
    logger.info("Combining all stock data...")
    combined = pd.concat(all_dfs, ignore_index=True)
    combined["Date"] = pd.to_datetime(combined["Date"])

    # Step 3 — check which partitions already exist in S3
    existing_partitions = get_existing_partitions()

    # Step 4 — group by date and write one file per trading day
    logger.info("Writing daily partitions to S3...")
    dates = combined["Date"].dt.date.unique()
    total_dates = len(dates)

    for i, date in enumerate(sorted(dates), 1):
        date_str = str(date)  # "2025-04-14"

        # Skip if already written — safe to re-run backfill anytime
        if date_str in existing_partitions:
            logger.info("[%d/%d] Skipping %s — already exists", i, total_dates, date_str)
            continue

        # Filter all stocks for this specific date
        day_df = combined[combined["Date"].dt.date == date].copy()

        if day_df.empty:
            continue

        # Build S3 partition path
        s3_key = (
            f"bronze/stocks/"
            f"year={date.year}/"
            f"month={date.month:02d}/"
            f"day={date.day:02d}/"
            f"data.parquet"
        )

        write_parquet_to_s3(day_df, s3_key)
        logger.info(
            "[%d/%d] Written %s → %d stocks",
            i, total_dates, date_str, len(day_df)
        )

    logger.info("Backfill complete!")


if __name__ == "__main__":
    main()