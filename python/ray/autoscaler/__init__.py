import logging
from logging import FileHandler

logger = logging.getLogger(__name__)
file_handler_info = FileHandler("test.log", mode="w")
file_handler_info.setLevel(logging.DEBUG)
logger.addHandler(file_handler_info)
