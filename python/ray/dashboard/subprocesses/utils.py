import os
from typing import TypeVar

K = TypeVar("K")
V = TypeVar("V")


def module_logging_filename(
    module_name: str, incarnation: int, logging_filename: str
) -> str:
    """
    Parse logging_filename = STEM EXTENSION,
    return STEM - MODULE_NAME - INCARNATION EXTENSION

    If logging_filename is empty, return empty. This means the logs go to stderr.

    Example:
    module_name = "TestModule"
    incarnation = 5
    logging_filename = "dashboard.log"
    STEM = "dashboard"
    EXTENSION = ".log"
    return "dashboard-TestModule-5.log"
    """
    if not logging_filename:
        return ""
    stem, extension = os.path.splitext(logging_filename)
    return f"{stem}-{module_name}-{incarnation}{extension}"
