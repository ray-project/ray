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


def get_all_interfaces_ip_for_node(node_ip: str) -> str:
    """Get the "all interfaces" address matching the node's IP family.

    ``get_all_interfaces_ip`` derives the wildcard from localhost resolution,
    which is IPv4-first on most hosts. Servers that are reached through the
    node IP must bind a wildcard of the same family instead, otherwise a node
    with an IPv6 IP keeps a listener that only accepts IPv4.

    Args:
        node_ip: The IP address the node advertises.

    Returns:
        ``"::1"`` for an IPv6 loopback node, the localhost address for any
        other localhost node, ``"::"`` for an IPv6 node, and ``"0.0.0.0"`` for
        an IPv4 node.
    """
    if node_ip == "::1":
        # get_localhost_ip() resolves IPv4 first, so a node advertising IPv6
        # loopback would otherwise bind IPv4 loopback.
        return "::1"
    if is_localhost(node_ip):
        return get_localhost_ip()
    return "::" if is_ipv6(node_ip) else "0.0.0.0"


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
