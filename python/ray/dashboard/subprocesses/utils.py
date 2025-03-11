import os
from typing import TypeVar, Literal

K = TypeVar("K")
V = TypeVar("V")

ResponseType = Literal["http", "stream", "websocket"]


def module_logging_filename(module_name: str, logging_filename: str) -> str:
    """
    Parse logging_filename = STEM EXTENSION,
    return STEM - MODULE_NAME EXTENSION

    Example:
    module_name = "TestModule"
    logging_filename = "dashboard.log"
    STEM = "dashboard"
    EXTENSION = ".log"
    return "dashboard-TestModule.log"
    """
    stem, extension = os.path.splitext(logging_filename)
    return f"{stem}-{module_name}{extension}"
