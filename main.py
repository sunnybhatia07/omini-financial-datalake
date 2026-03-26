from ingestion.fetch_stocks_financial import main as fetch_financials
from ingestion.fetch_stocks_pricing import main as fetch_pricing
from utils.logger import get_logger

logger = get_logger(__name__)


def main():
    logger.info("Starting pipeline: Step 1/2 - Pricing Ingestion")
    fetch_pricing()

    logger.info("Starting pipeline: Step 2/2 - Financials Ingestion")
    fetch_financials()

    logger.info("Data lake pipeline complete.")


if __name__ == "__main__":
    main()