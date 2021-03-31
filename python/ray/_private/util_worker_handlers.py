import pathlib
from typing import List
import logging

# Logger for this module. It should be configured at the entry point
# into the program using Ray. Ray configures it by default automatically
# using logging.basicConfig in its entry/init points.
logger = logging.getLogger(__name__)
"""This module is to handle requests from raylet to util_worker."""


def dispatch(request: str, args: List[str]):
    """Dispatch the request to actual function to execute it.

    This is the entry point of this module. It'll be the link between py-module
    and raylet. Raylet will send request to this module to delegate some work
    to another process. Right now the request and arguments are passed by
    string.

    Args:
        request (str): The request id. Right now it's hard coded string.
            "DEL_FILE": Delete a file on local nodes. args should be the
            name of the file
        args (list[str]): The arguments of the request.

    """
    if request == "DEL_FILE" and len(args) == 1:
        path = pathlib.Path(args[0])
        if path.exists():
            path.unlink()
