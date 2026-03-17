from pathlib import Path

from nsepython import nse_eq_symbols

from utils.decorators import log_execution
from utils.logger import get_logger

logger = get_logger(__name__)

OUTPUT_PATH = Path("data") / "stocks_list.csv"


@log_execution
def fetch_stock_list():
    logger.info("Fetching NSE equity symbols")

    stocks = nse_eq_symbols()

    if not stocks:
        logger.warning("No stock symbols retrieved from NSE")
        return []

    logger.info("Fetched %d stock symbols", len(stocks))
    return stocks


@log_execution
def save_stock_list(symbols):
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    with OUTPUT_PATH.open("w", encoding="utf-8") as file:
        file.write("Symbol\n")
        file.writelines(f"{symbol}\n" for symbol in symbols)

    logger.info("Saved %d symbols to %s", len(symbols), OUTPUT_PATH)


def main():
    symbols = fetch_stock_list()

    if not symbols:
        logger.error("Stock list is empty. Aborting save.")
        return

    logger.info("First 20 symbols: %s", symbols[:20])

    save_stock_list(symbols)


if __name__ == "__main__":
    main()
