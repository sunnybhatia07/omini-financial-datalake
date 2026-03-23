from nsepython import nse_eq_symbols

from utils.decorators import log_execution
from utils.logger import get_logger

logger = get_logger(__name__)


@log_execution
def fetch_stock_list() -> list:
    logger.info("Fetching real-time NSE equity symbols")
    
    stocks = nse_eq_symbols()

    if not stocks:
        logger.warning("No stock symbols retrieved from NSE")
        return []

    logger.info("Fetched %d stock symbols", len(stocks))
    return stocks


def main():
    symbols = fetch_stock_list()

    if not symbols:
        logger.error("Stock list is empty. Aborting.")
        return

    logger.info("First 20 symbols: %s", symbols[:20])


if __name__ == "__main__":
    main()