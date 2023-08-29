import logging
from rich.logging import RichHandler

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def add_handlers(logger: logging.Logger):
    handler = RichHandler()
    formatter = logging.Formatter(
        fmt="[%(levelname)s %(asctime)s] %(filename)s:%(lineno)d - %(message)s"
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)

if not logger.hasHandlers():
    add_handlers(logger)