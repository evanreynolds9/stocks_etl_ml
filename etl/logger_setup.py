import logging

logging.basicConfig(
    filename='etl.log',
    level=logging.INFO,
    format="%(asctime)s:%(levelname)s:%(name)s:%(funcName)s:%(module)s:line %(lineno)d:%(message)s"
)

logger = logging.getLogger("etl")

# Setup yfinance logger
yf_logger = logging.getLogger("yfinance")
yf_logger.setLevel(logging.WARNING)