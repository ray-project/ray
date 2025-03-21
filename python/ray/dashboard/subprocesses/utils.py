import os
import enum
from typing import TypeVar
from ray._private.utils import validate_socket_filepath

K = TypeVar("K")
V = TypeVar("V")


class ResponseType(enum.Enum):
    HTTP = "http"
    STREAM = "stream"
    WEBSOCKET = "websocket"


def module_logging_filename(
    module_name: str, logging_filename: str, is_stderr=False
) -> str:
    """
    Parse logging_filename = STEM EXTENSION,
    return STEM _ MODULE_NAME _ EXTENSION

    If is_stderr is True, EXTENSION is ".err"

    Example:
    module_name = "TestModule"
    logging_filename = "dashboard.log"
    STEM = "dashboard"
    EXTENSION = ".log"
    return "dashboard_TestModule.log"
    """
    stem, extension = os.path.splitext(logging_filename)
    if is_stderr:
        extension = ".err"
    return f"{stem}_{module_name}{extension}"


def get_socket_path(socket_dir: str, module_name: str) -> str:
    socket_path = os.path.join(socket_dir, "dash_" + module_name)
    validate_socket_filepath(socket_path)
    return socket_path


def get_named_pipe_path(module_name: str) -> str:
    return r"\\.\pipe\dash_" + module_name
