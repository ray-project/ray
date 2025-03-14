import os
import enum
from typing import TypeVar

K = TypeVar("K")
V = TypeVar("V")


class ResponseType(enum.Enum):
    HTTP = "http"
    STREAM = "stream"
    WEBSOCKET = "websocket"


def module_logging_filename(module_name: str, logging_filename: str) -> str:
    """
    Parse logging_filename = STEM EXTENSION,
    return STEM _ MODULE_NAME EXTENSION

    Example:
    module_name = "TestModule"
    logging_filename = "dashboard.log"
    STEM = "dashboard"
    EXTENSION = ".log"
    return "dashboard-TestModule.log"
    """
    stem, extension = os.path.splitext(logging_filename)
    return f"{stem}_{module_name}{extension}"
