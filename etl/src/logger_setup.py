# Use loguru for logging
from loguru import logger
from pathlib import Path
import dotenv
import os

dotenv_file = dotenv.find_dotenv()
dotenv.load_dotenv(dotenv_file)

# Get file path
BASE_DIR = Path(__file__).resolve().parent
relative = os.getenv("LOG_FILE_PATH", "../etl.log")
log_file_path = BASE_DIR / relative

# Remove default setup
logger.remove(0)

# Add format
logger.add(
    log_file_path,
    level="INFO",
    format="{time: YYYY-MM-DD HH:mm:ss}:{module}:{function}:{level}:{message}",
    enqueue=True,
)

# Make module importable
__all__ = ["logger"]