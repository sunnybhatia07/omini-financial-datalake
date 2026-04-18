from datetime import datetime
import pytz
import yfinance as yf

from .fetch_stocks_pricing import main as fetch_pricing
from utils.logger import get_logger

logger = get_logger(__name__)


def did_market_trade_today() -> bool:
    ist = pytz.timezone("Asia/Kolkata")
    today = datetime.now(ist).date()

    try:
        df = yf.Ticker("^NSEI").history(period="5d")
    except Exception:
        return False

    if df.empty:
        return False

    return df.index[-1].date() == today


def main():
    if did_market_trade_today():
        logger.info("Starting pipeline: Pricing Ingestion")
        fetch_pricing()
        logger.info("Data lake pipeline complete.")
    else:
        logger.info("Market did not trade today. Skipping pricing ingestion.")
        logger.info("Data lake pipeline complete.")


if __name__ == "__main__":
    main()