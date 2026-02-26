import logging
import os
from typing import Optional

from dotenv import load_dotenv

load_dotenv()

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()


class ColorFormatter(logging.Formatter):
    COLORS = {
        "DEBUG": "\033[1;36m",  # Bold Cyan
        "INFO": "\033[1;32m",  # Bold Green
        "WARNING": "\033[1;33m",  # Bold Yellow
        "ERROR": "\033[1;31m",  # Bold Red
        "CRITICAL": "\033[1;41m",  # Bold background red
    }
    RESET = "\033[0m"

    def format(self, record):  # type: ignore[override]
        color = self.COLORS.get(record.levelname, self.RESET)
        message = super().format(record)
        return f"{color}{message}{self.RESET}"


class Logger:
    def __init__(self, name: Optional[str] = None):
        level = getattr(logging, LOG_LEVEL, logging.INFO)
        self.logger = logging.getLogger(name or "app_logger")
        self.logger.setLevel(level)
        self.logger.propagate = False  # Critical to avoid duplication from root logger

        if not self.logger.hasHandlers():
            handler = logging.StreamHandler()
            formatter = ColorFormatter(
                "%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(funcName)s - %(message)s"
            )
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)

    def get_logger(self):
        return self.logger
