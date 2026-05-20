import argparse
import time
from datetime import datetime

import pandas as pd
import pytz
import yfinance as yf

from ingestion.fetch_stocks_list import fetch_stock_list
from utils.logger import get_logger
from utils.s3_helper import write_parquet_to_s3, list_s3_keys

logger = get_logger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
BATCH_SIZE = 50   # smaller batch for backfill — more stable for long history
SLEEP_TIME = 3    # slightly longer sleep for backfill to avoid throttling

# Valid yfinance periods
VALID_PERIODS = ["1d", "5d", "1mo", "3mo", "6mo", "1y", "2y", "5y", "10y", "ytd", "max"]


def chunk_list(lst: list, size: int):
    """Split a list into chunks of given size."""
    for i in range(0, len(lst), size):
        yield lst[i:i + size]


def get_existing_partitions() -> set:
    """
    Get all date partitions already written to S3.
    Returns a set of strings like {'2025-04-14', '2025-04-13'}
    So we never overwrite already completed days.
    """
    keys = list_s3_keys("bronze/stocks/")
    existing = set()

    for key in keys:
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


def fetch_batch_history(symbols: list, period: str) -> pd.DataFrame | None:
    """
    Fetch full history for a batch of stocks in one API call.
    Returns combined DataFrame for all stocks in the batch.
    """
    tickers = " ".join([f"{s}.NS" for s in symbols])

    try:
        df = yf.download(
            tickers=tickers,
            period=period,
            auto_adjust=False,
            group_by="ticker",
            progress=False,
            threads=True,
        )

        if df.empty:
            logger.warning("Empty response for batch starting with: %s", symbols[:3])
            return None

        all_rows = []

        if len(symbols) == 1:
            # Single ticker — flat columns
            symbol = symbols[0]
            df = df.reset_index()
            df["Date"] = pd.to_datetime(df["Date"]).dt.tz_localize(None)
            df["Symbol"] = symbol
            all_rows.append(df)

        else:
            # Multiple tickers — MultiIndex columns
            for symbol in symbols:
                ticker = f"{symbol}.NS"
                try:
                    stock_df = df[ticker].copy()
                    stock_df = stock_df.reset_index()
                    stock_df["Date"] = pd.to_datetime(stock_df["Date"]).dt.tz_localize(None)
                    stock_df["Symbol"] = symbol
                    if not stock_df.empty:
                        all_rows.append(stock_df)
                except KeyError:
                    logger.warning("No data for %s in batch response", symbol)
                    continue

        if not all_rows:
            return None

        return pd.concat(all_rows, ignore_index=True)

    except Exception as e:
        logger.error("Batch fetch failed: %s", e)
        return None


def main():
    parser = argparse.ArgumentParser(description="Omini Bronze Backfill")
    parser.add_argument(
        "--period",
        type=str,
        default="5y",
        choices=VALID_PERIODS,
        help=(
            "yfinance period to fetch. "
            "Options: 1d, 5d, 1mo, 3mo, 6mo, 1y, 2y, 5y, 10y, ytd, max. "
            "Default: 5y"
        )
    )
    args = parser.parse_args()

    logger.info("========================================")
    logger.info("Bronze Backfill — Period: %s", args.period)
    logger.info("========================================")

    symbols = fetch_stock_list()

    if not symbols:
        logger.error("No symbols retrieved. Aborting backfill.")
        return

    total = len(symbols)
    batches = list(chunk_list(symbols, BATCH_SIZE))
    total_batches = len(batches)

    logger.info("Total stocks: %d | Batch size: %d | Total batches: %d",
                total, BATCH_SIZE, total_batches)

    # Step 1 — fetch all stock histories in batches
    all_dfs = []
    success_count = 0
    fail_count = 0

    for i, batch in enumerate(batches, 1):
        logger.info("[Batch %d/%d] Fetching %d stocks...", i, total_batches, len(batch))

        df = fetch_batch_history(batch, args.period)

        if df is not None and not df.empty:
            stocks_returned = df["Symbol"].nunique()
            success_count += stocks_returned
            fail_count += len(batch) - stocks_returned
            all_dfs.append(df)
            logger.info("[Batch %d/%d] ✓ %d/%d stocks returned",
                        i, total_batches, stocks_returned, len(batch))
        else:
            fail_count += len(batch)
            logger.warning("[Batch %d/%d] ✗ No data returned", i, total_batches)

        if i < total_batches:
            time.sleep(SLEEP_TIME)

    if not all_dfs:
        logger.error("No data fetched. Aborting.")
        return

    # Step 2 — combine all batches
    logger.info("Combining all stock data...")
    combined = pd.concat(all_dfs, ignore_index=True)
    combined["Date"] = pd.to_datetime(combined["Date"])

    # Keep only required columns
    raw_columns = ["Date", "Symbol", "Open", "High", "Low", "Close", "Adj Close", "Volume"]
    combined = combined[[c for c in raw_columns if c in combined.columns]]

    # Step 3 — check existing partitions to avoid duplicates
    existing_partitions = get_existing_partitions()

    # Step 4 — group by date and write one file per trading day
    logger.info("Writing daily partitions to S3...")
    dates = combined["Date"].dt.date.unique()
    total_dates = len(dates)
    written_count = 0
    skipped_count = 0

    for i, date in enumerate(sorted(dates), 1):
        date_str = str(date)

        if date_str in existing_partitions:
            logger.info("[%d/%d] Skipping %s — already exists", i, total_dates, date_str)
            skipped_count += 1
            continue

        day_df = combined[combined["Date"].dt.date == date].copy()

        if day_df.empty:
            continue

        s3_key = (
            f"bronze/stocks/"
            f"year={date.year}/"
            f"month={date.month:02d}/"
            f"day={date.day:02d}/"
            f"data.parquet"
        )

        write_parquet_to_s3(day_df, s3_key)
        written_count += 1
        logger.info("[%d/%d] Written %s → %d stocks",
                    i, total_dates, date_str, len(day_df))

    logger.info("========================================")
    logger.info("Backfill complete!")
    logger.info("Stocks — Success: %d | Failed: %d", success_count, fail_count)
    logger.info("Dates  — Written: %d | Skipped: %d", written_count, skipped_count)
    logger.info("========================================")


if __name__ == "__main__":
    main()