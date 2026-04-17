import time
from datetime import datetime

import pandas as pd
import numpy as np
import pytz
import yfinance as yf

from ingestion.fetch_stocks_list import fetch_stock_list
from utils.decorators import log_execution
from utils.logger import get_logger
from utils.s3_helper import write_parquet_to_s3

logger = get_logger(__name__)

# Fields we want from ticker.info
FUNDAMENTAL_FIELDS = [
    "symbol",
    "marketCap",
    "trailingPE",
    "forwardPE",
    "priceToBook",
    "debtToEquity",
    "returnOnEquity",
    "revenueGrowth",
    "earningsGrowth",
    "beta",
    "fiftyTwoWeekHigh",
    "fiftyTwoWeekLow",
    "averageVolume",
    "shortRatio",
    "bookValue",
    "earningsPerShare",
    "dividendYield",
    "payoutRatio",
    "currentRatio",
    "quickRatio",
    "totalDebt",
    "totalRevenue",
    "grossMargins",
    "operatingMargins",
    "profitMargins",
]


def process_fundamental(symbol: str) -> dict | None:
    """
    Fetch fundamental data for a single stock from yfinance.
    Returns a flat dictionary of fundamental fields.
    """
    try:
        ticker = yf.Ticker(f"{symbol}.NS")
        info = ticker.info

        if not info or len(info) < 5:
            logger.warning("Empty or invalid info for %s, skipping.", symbol)
            return None

        row = {"symbol": symbol}

        for field in FUNDAMENTAL_FIELDS[1:]:  # skip 'symbol' — already added
            row[field] = info.get(field, None)

        return row

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
    all_rows = []
    success_count = 0
    fail_count = 0
    failed_symbols = []

    logger.info("Starting fundamentals ingestion for %d stocks...", total)

    for i, symbol in enumerate(symbols, 1):
        row = process_fundamental(symbol)

        if row is not None:
            all_rows.append(row)
            success_count += 1
        else:
            fail_count += 1
            failed_symbols.append(symbol)

        logger.info(
            "[%d/%d] %s | Success: %d | Failed: %d | Remaining: %d",
            i, total, symbol, success_count, fail_count, total - i
        )

        time.sleep(1)  # avoid yfinance rate limiting

    if not all_rows:
        logger.error("No fundamental data collected. Nothing written to S3.")
        return

    # Build DataFrame
    df = pd.DataFrame(all_rows)

    # Add ingestion timestamp
    ist = pytz.timezone("Asia/Kolkata")
    today = datetime.now(ist).date()
    df["date"] = today

    df = df.replace([float('inf'), float('-inf'), 'Infinity', '-Infinity'], None)
    numeric_cols = [col for col in df.columns if col not in ['symbol', 'date']]
    df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors='coerce')

    # Build S3 partition path
    s3_key = (
        f"bronze/fundamentals/"
        f"year={today.year}/"
        f"month={today.month:02d}/"
        f"day={today.day:02d}/"
        f"data.parquet"
    )

    write_parquet_to_s3(df, s3_key)

    logger.info(
        "Fundamentals bronze complete | Total: %d | Success: %d | Failed: %d",
        total, success_count, fail_count
    )

    if failed_symbols:
        logger.warning(
            "Failed symbols (%d): %s", len(failed_symbols), failed_symbols
        )


if __name__ == "__main__":
    main()