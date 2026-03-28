import time
from pathlib import Path

import pandas as pd
import yfinance as yf

from ingestion.fetch_stocks_list import fetch_stock_list
from utils.decorators import log_execution
from utils.logger import get_logger

logger = get_logger(__name__)


@log_execution
def process_financials(symbol: str) -> None:
    yf_symbol = f"{symbol}.NS"
    file_path = Path("data/financials/raw") / f"{symbol}_financials.csv"

    ticker = yf.Ticker(yf_symbol)

    try:
        inc_stmt = ticker.quarterly_income_stmt.T
        bal_sheet = ticker.quarterly_balance_sheet.T
        cash_flow = ticker.quarterly_cash_flow.T
    except Exception as e:
        raise ValueError(f"Failed to pull raw financial statements: {e}")

    if inc_stmt.empty and bal_sheet.empty:
        raise ValueError("No financial data found")

    df_combined = pd.concat([inc_stmt, bal_sheet, cash_flow], axis=1)
    df_combined = df_combined.loc[:, ~df_combined.columns.duplicated()]

    df_combined = df_combined.reset_index()
    df_combined.rename(columns={"index": "Date"}, inplace=True)

    df_combined["Date"] = pd.to_datetime(df_combined["Date"]).dt.tz_localize(None)
    df_combined.insert(1, "Symbol", symbol)

    desired_columns = [
        "Date",
        "Symbol",
        "Total Revenue",
        "Gross Profit",
        "Operating Income",
        "Net Income",
        "Basic EPS",
        "Total Assets",
        "Total Debt",
        "Total Liabilities Net Minority Interest",
        "Stockholders Equity",
        "Operating Cash Flow",
        "Free Cash Flow",
    ]

    final_columns = [col for col in desired_columns if col in df_combined.columns]
    df_final = df_combined[final_columns].copy()

    file_path.parent.mkdir(parents=True, exist_ok=True)
    df_final.to_csv(file_path, index=False)


def main():
    symbols = fetch_stock_list()

    if not symbols:
        logger.error("Failed to retrieve live stock list. Aborting financials fetch.")
        return

    total_symbols = len(symbols)
    success_count = 0
    fail_count = 0
    failed_symbols = []

    logger.info("Starting financials fetch for %d stocks...", total_symbols)

    for i, symbol in enumerate(symbols, 1):
        remaining = total_symbols - i

        try:
            process_financials(symbol)
            success_count += 1
            logger.info(
                "[%d/%d] Processed %s | Success: %d | Failed: %d | Remaining: %d",
                i, total_symbols, symbol, success_count, fail_count, remaining
            )
            time.sleep(1.5)

        except Exception as e:
            fail_count += 1
            failed_symbols.append(symbol)
            logger.error(
                "[%d/%d] Failed %s: %s | Success: %d | Failed: %d | Remaining: %d",
                i, total_symbols, symbol, e, success_count, fail_count, remaining
            )

    logger.info(
        "Financials Fetch Complete! Total: %d | Success: %d | Failed: %d",
        total_symbols, success_count, fail_count
    )

    if failed_symbols:
        logger.warning(
            "Failed financial symbols (%d): %s",
            len(failed_symbols),
            failed_symbols
        )
    else:
        logger.info("No financial ingestion failures detected.")


if __name__ == "__main__":
    main()