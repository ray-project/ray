import enum
import os
import sys
from typing import TypeVar

import aiohttp

from ray._private.utils import validate_socket_filepath

K = TypeVar("K")
V = TypeVar("V")


class ResponseType(enum.Enum):
    HTTP = "http"
    STREAM = "stream"
    WEBSOCKET = "websocket"


def module_logging_filename(
    module_name: str, logging_filename: str, extension: str = ""
) -> str:
    """
    Parse logging_filename = STEM EXTENSION,
    return STEM _ MODULE_NAME _ EXTENSION

    If logging_filename is empty, return empty string.
    If extension is empty, use the extension from logging_filename.

    Example:
    module_name = "TestModule"
    logging_filename = "dashboard.log"
    STEM = "dashboard"
    EXTENSION = ".log"
    return "dashboard_TestModule.log"
    """
    if not logging_filename:
        return ""
    stem, ext = os.path.splitext(logging_filename)
    if not extension:
        extension = ext
    return f"{stem}_{module_name}{extension}"


def get_socket_path(socket_dir: str, module_name: str) -> str:
    socket_path = os.path.join(socket_dir, "dash_" + module_name)
    validate_socket_filepath(socket_path)
    return socket_path


def get_named_pipe_path(module_name: str, session_name: str) -> str:
    return r"\\.\pipe\dash_" + module_name + "_" + session_name


def get_http_session_to_module(
    module_name: str, socket_dir: str, session_name: str
) -> aiohttp.ClientSession:
    """
    Get the aiohttp http client session to the subprocess module.
    """
    if sys.platform == "win32":
        named_pipe_path = get_named_pipe_path(module_name, session_name)
        connector = aiohttp.NamedPipeConnector(named_pipe_path)
    else:
        socket_path = get_socket_path(socket_dir, module_name)
        connector = aiohttp.UnixConnector(socket_path)
    return aiohttp.ClientSession(connector=connector)
