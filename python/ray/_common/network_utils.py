import socket
from contextlib import closing

from ray._raylet import (  # noqa: F401
    build_address,
    get_all_interfaces_ip,
    get_localhost_ip,
    is_ipv6,
    node_ip_address_from_perspective,
    parse_address,
)


def is_localhost(host: str) -> bool:
    """Check if the given host string represents a localhost address.

    Args:
        host: The hostname or IP address to check.

    Returns:
        True if the host is a localhost address, False otherwise.
    """
    return host in ("localhost", "127.0.0.1", "::1")


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
