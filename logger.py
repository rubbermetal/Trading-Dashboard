import os
import logging
from logging.handlers import RotatingFileHandler

_LOG_DIR = os.path.join(os.path.dirname(__file__), 'logs')
_LOG_FILE = os.path.join(_LOG_DIR, 'dashboard.log')
_MAX_BYTES = 10 * 1024 * 1024  # 10 MB
_BACKUP_COUNT = 3
_FORMAT = '%(asctime)s [%(name)s] %(levelname)s %(message)s'

os.makedirs(_LOG_DIR, exist_ok=True)

_handler = RotatingFileHandler(_LOG_FILE, maxBytes=_MAX_BYTES, backupCount=_BACKUP_COUNT)
_handler.setFormatter(logging.Formatter(_FORMAT))

_console = logging.StreamHandler()
_console.setFormatter(logging.Formatter(_FORMAT))


def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.handlers:
        logger.setLevel(logging.DEBUG)
        logger.addHandler(_handler)
        logger.addHandler(_console)
    return logger
