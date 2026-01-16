import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime

# Generate timestamp string
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
LOG_FILE = f"V3_Upgrade_Validation_{timestamp}.log"

def get_logger(name: str):
    logger = logging.getLogger(name)

    # Prevent duplicate logs
    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    )

    file_handler = RotatingFileHandler(
        LOG_FILE,
        maxBytes=5_000_000,
        backupCount=3
    )
    file_handler.setFormatter(formatter)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger
