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
    return STEM _ MODULE_NAME _ EXTENSION

    If logging_filename is empty, return "stderr"

    Example:
    module_name = "TestModule"
    logging_filename = "dashboard.log"
    STEM = "dashboard"
    EXTENSION = ".log"
    return "dashboard_TestModule.log"
    """
    if not logging_filename:
        return "stderr"
    stem, extension = os.path.splitext(logging_filename)
    return f"{stem}_{module_name}{extension}"


def get_socket_path(socket_dir: str, module_name: str) -> str:
    socket_path = os.path.join(socket_dir, "dashboard_" + module_name)
    # The max length of a Unix socket path is 108 bytes.
    if len(socket_path) > 108:
        # Discard the "dashboard_" prefix.
        socket_path = os.path.join(socket_dir, module_name)
    if len(socket_path) > 108:
        raise ValueError(
            f"Socket path {socket_path} is too long. "
            "Please use a shorter module name or a shorter socket_dir."
        )
    return socket_path
