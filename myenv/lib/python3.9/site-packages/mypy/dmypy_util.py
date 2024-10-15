"""Shared code between dmypy.py and dmypy_server.py.

This should be pretty lightweight and not depend on other mypy code (other than ipc).
"""

from __future__ import annotations

import json
from typing import Any, Final, Iterable

from mypy.ipc import IPCBase

DEFAULT_STATUS_FILE: Final = ".dmypy.json"


def receive(connection: IPCBase) -> Any:
    """Receive single JSON data frame from a connection.

    Raise OSError if the data received is not valid JSON or if it is
    not a dict.
    """
    bdata = connection.read()
    if not bdata:
        raise OSError("No data received")
    try:
        data = json.loads(bdata)
    except Exception as e:
        raise OSError("Data received is not valid JSON") from e
    if not isinstance(data, dict):
        raise OSError(f"Data received is not a dict ({type(data)})")
    return data


def send(connection: IPCBase, data: Any) -> None:
    """Send data to a connection encoded and framed.

    The data must be JSON-serializable. We assume that a single send call is a
    single frame to be sent on the connect.
    """
    connection.write(json.dumps(data))


class WriteToConn:
    """Helper class to write to a connection instead of standard output."""

    def __init__(self, server: IPCBase, output_key: str = "stdout"):
        self.server = server
        self.output_key = output_key

    def write(self, output: str) -> int:
        resp: dict[str, Any] = {}
        resp[self.output_key] = output
        send(self.server, resp)
        return len(output)

    def writelines(self, lines: Iterable[str]) -> None:
        for s in lines:
            self.write(s)
