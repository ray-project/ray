import socket
from contextlib import closing

from ray._raylet import (  # noqa: F401
    build_address,
    get_all_interfaces_ip,
    get_localhost_ip,
    is_ipv6,
    is_localhost,
    node_ip_address_from_perspective,
    parse_address,
)


def find_free_port(family: socket.AddressFamily = socket.AF_INET) -> int:
    """Find a free port on the local machine.

    Args:
        family: The socket address family (AF_INET for IPv4, AF_INET6 for IPv6).
            Defaults to AF_INET.

    Returns:
        An available port number.
    """
    with closing(socket.socket(family, socket.SOCK_STREAM)) as s:
        s.bind(("", 0))
        return s.getsockname()[1]
