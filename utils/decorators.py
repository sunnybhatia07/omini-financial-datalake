from functools import wraps
from utils.logger import get_logger

logger = get_logger(__name__)


def log_execution(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        logger.info("Starting %s", func.__name__)
        result = func(*args, **kwargs)
        logger.info("Finished %s", func.__name__)
        return result

    return wrapper