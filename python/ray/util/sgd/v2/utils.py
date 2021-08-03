from contextlib import closing
import socket
from typing import Tuple

import ray


def get_address_and_port() -> Tuple[str, int]:
    """Returns the IP address and a free port on this node."""
    addr = ray.util.get_node_ip_address()

    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.bind(("", 0))
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        port = s.getsockname()[1]

    return addr, port
