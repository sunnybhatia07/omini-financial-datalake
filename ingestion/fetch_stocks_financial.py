from pathlib import Path

import pandas as pd
import yfinance as yf

from utils.decorators import log_execution
from utils.logger import get_logger

logger = get_logger(__name__)


@log_execution
def process_financials(symbol: str) -> None:
    yf_symbol = f"{symbol}.NS"
    file_path = Path("data/financials/raw") / f"{symbol}_financials.csv"

    logger.info("Fetching quarterly financials for %s", symbol)

    ticker = yf.Ticker(yf_symbol)

    try:
        inc_stmt = ticker.quarterly_income_stmt.T
        bal_sheet = ticker.quarterly_balance_sheet.T
        cash_flow = ticker.quarterly_cash_flow.T
    except Exception as e:
        logger.error("Failed to pull raw financial statements for %s: %s", symbol, e)
        return

    if inc_stmt.empty and bal_sheet.empty:
        logger.warning("No financial data found for %s", symbol)
        return

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
        "Free Cash Flow"
    ]

    final_columns = [col for col in desired_columns if col in df_combined.columns]
    df_final = df_combined[final_columns].copy()

    file_path.parent.mkdir(parents=True, exist_ok=True)

    df_final.to_csv(file_path, index=False)
    logger.info("Updated %s with latest financial statements", file_path)


def main():
    process_financials("TCS")


if __name__ == "__main__":
    main()