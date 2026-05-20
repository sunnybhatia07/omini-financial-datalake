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

# ── Config ────────────────────────────────────────────────────────────────────
BATCH_SIZE = 100  # number of stocks per yfinance batch request
SLEEP_TIME = 2    # seconds between batches to avoid throttling


def chunk_list(lst: list, size: int):
    """Split a list into chunks of given size."""
    for i in range(0, len(lst), size):
        yield lst[i:i + size]


def fetch_batch(symbols: list, today) -> pd.DataFrame | None:
    """
    Fetch OHLCV data for a batch of stocks in one API call.

    yfinance.download() fetches multiple tickers in one HTTP request
    instead of one request per stock — much faster and less throttling risk.

    Returns combined DataFrame for all stocks in the batch.
    """
    tickers = " ".join([f"{s}.NS" for s in symbols])

    try:
        df = yf.download(
            tickers=tickers,
            period="5d",
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
            df_today = df[df["Date"].dt.date == today]
            if not df_today.empty:
                all_rows.append(df_today)

        else:
            # Multiple tickers — MultiIndex columns
            for symbol in symbols:
                ticker = f"{symbol}.NS"
                try:
                    stock_df = df[ticker].copy()
                    stock_df = stock_df.reset_index()
                    stock_df["Date"] = pd.to_datetime(stock_df["Date"]).dt.tz_localize(None)
                    stock_df["Symbol"] = symbol
                    df_today = stock_df[stock_df["Date"].dt.date == today]
                    if not df_today.empty:
                        all_rows.append(df_today)
                except KeyError:
                    logger.warning("No data for %s in batch response", symbol)
                    continue

        if not all_rows:
            return None

        return pd.concat(all_rows, ignore_index=True)

    except Exception as e:
        logger.error("Batch fetch failed: %s", e)
        return None


@log_execution
def main():
    symbols = fetch_stock_list()

    if not symbols:
        logger.error("Failed to retrieve stock list. Aborting.")
        return

    total = len(symbols)
    batches = list(chunk_list(symbols, BATCH_SIZE))
    total_batches = len(batches)

    ist = pytz.timezone("Asia/Kolkata")
    today = datetime.now(ist).date()

    logger.info("Starting batch bronze ingestion...")
    logger.info("Total stocks: %d | Batch size: %d | Total batches: %d",
                total, BATCH_SIZE, total_batches)

    all_dfs = []
    success_count = 0
    fail_count = 0

    for i, batch in enumerate(batches, 1):
        logger.info("[Batch %d/%d] Fetching %d stocks...", i, total_batches, len(batch))

        df = fetch_batch(batch, today)

        if df is not None and not df.empty:
            # Count per stock accurately
            stocks_returned = df["Symbol"].nunique()
            success_count += stocks_returned
            fail_count += len(batch) - stocks_returned

            all_dfs.append(df)
            logger.info("[Batch %d/%d] ✓ %d/%d stocks returned",
                        i, total_batches, stocks_returned, len(batch))
        else:
            # Entire batch failed
            fail_count += len(batch)
            logger.warning("[Batch %d/%d] ✗ No data returned for entire batch",
                           i, total_batches)

        if i < total_batches:
            time.sleep(SLEEP_TIME)

    if not all_dfs:
        logger.error("No data collected today. Nothing written to S3.")
        return

    combined_df = pd.concat(all_dfs, ignore_index=True)

    # Keep only required columns
    raw_columns = ["Date", "Symbol", "Open", "High", "Low", "Close", "Adj Close", "Volume"]
    combined_df = combined_df[[c for c in raw_columns if c in combined_df.columns]]

    # Write to S3
    s3_key = (
        f"bronze/stocks/"
        f"year={today.year}/"
        f"month={today.month:02d}/"
        f"day={today.day:02d}/"
        f"data.parquet"
    )

    write_parquet_to_s3(combined_df, s3_key)

    logger.info("========================================")
    logger.info("Bronze ingestion complete!")
    logger.info("Success: %d | Failed: %d | Total: %d | Rows written: %d",
                success_count, fail_count, total, len(combined_df))
    logger.info("========================================")


if __name__ == "__main__":
    main()