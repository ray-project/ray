from typing import Optional, Tuple, Union
import socket
from functools import lru_cache

from ray._raylet import (
    build_address as _build_address,
    is_ipv6_ip as _is_ipv6_ip,
    node_ip_address_from_perspective as _node_ip_address_from_perspective,
    parse_address as _parse_address,
)


def parse_address(address: str) -> Optional[Tuple[str, str]]:
    """Parse a network address string into host and port.

    Args:
        address: The address string to parse (e.g., "localhost:8000", "[::1]:8000").

    Returns:
        Tuple with (host, port) if port found, None if no colon separator.
    """
    return _parse_address(address)


def build_address(host: str, port: Union[int, str]) -> str:
    """Build a network address string from host and port.

    Args:
        host: The hostname or IP address.
        port: The port number (int or string).

    Returns:
        Formatted address string (e.g., "localhost:8000" or "[::1]:8000").
    """
    return _build_address(host, port)


def node_ip_address_from_perspective(address: str = "") -> str:
    """IP address by which the local node can be reached *from* the `address`.

    If no address is given, defaults to public DNS servers for detection. For
    performance, the result is cached when using the default address (empty string).
    When a specific address is provided, detection is performed fresh every time.

    Args:
        address: The IP address and port of any known live service on the
            network you care about.

    Returns:
        The IP address by which the local node can be reached from the address.
    """
    return _node_ip_address_from_perspective(address)


def is_ipv6_ip(ip: str) -> bool:
    """Check if an IP string is IPv6 format.

    Args:
        ip: The IP address string to check (must be pure IP, no port).

    Returns:
        True if the IP is IPv6, False if IPv4.
    """
    return _is_ipv6_ip(ip)


@lru_cache(maxsize=1)
def get_localhost_address() -> str:
    """Get localhost loopback address with IPv4/IPv6 support.

    Returns:
        The localhost loopback IP address.
    """
    # Try IPv4 first, then IPv6 localhost resolution
    for family in [socket.AF_INET, socket.AF_INET6]:
        try:
            dns_result = socket.getaddrinfo(
                "localhost", None, family, socket.SOCK_STREAM
            )
            return dns_result[0][4][0]
        except socket.gaierror:
            continue

    # Final fallback to IPv4 loopback
    return "127.0.0.1"


def is_localhost(host: str) -> bool:
    """Check if the given host string represents a localhost address.

    Args:
        host: The hostname or IP address to check.

    Returns:
        True if the host is a localhost address, False otherwise.
    """
    return host in ("localhost", "127.0.0.1", "::1")


def create_socket(socket_type: int = socket.SOCK_STREAM) -> socket.socket:
    """Create a Python socket object with the appropriate family based on the node IP.

    This function automatically gets the node IP address and creates a socket
    with the correct family (AF_INET for IPv4, AF_INET6 for IPv6).

    Args:
        socket_type: The socket type (socket.SOCK_STREAM, socket.SOCK_DGRAM, etc.).

    Returns:
        A Python socket.socket object configured for the node's IP family.

    Example:
        # Create a TCP socket for the current node
        sock = create_socket()

        # Create a UDP socket for the current node
        sock = create_socket(socket.SOCK_DGRAM)
    """
    node_ip = node_ip_address_from_perspective()
    family = socket.AF_INET6 if is_ipv6_ip(node_ip) else socket.AF_INET

    return socket.socket(family, socket_type)
