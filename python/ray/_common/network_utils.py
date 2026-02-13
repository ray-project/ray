import socket
from contextlib import closing
from functools import lru_cache
from typing import Optional, Tuple, Union

from ray._raylet import (
    build_address as _build_address,
    is_ipv6 as _is_ipv6,
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


def node_ip_address_from_perspective(address: Optional[str] = None) -> str:
    """IP address by which the local node can be reached *from* the `address`.

    If no address is given, defaults to public DNS servers for detection.

    Args:
        address: The IP address and port of any known live service on the
            network you care about.

    Returns:
        The IP address by which the local node can be reached from the address.
    """
    return _node_ip_address_from_perspective(address)


def is_ipv6(host: str) -> bool:
    """Check if a host is resolved to IPv6.

    Args:
        host: The IP or domain name to check (must be without port).

    Returns:
        True if the host is resolved to IPv6, False if IPv4.
    """
    return _is_ipv6(host)


@lru_cache(maxsize=1)
def get_localhost_ip() -> str:
    """Get localhost loopback ip with IPv4/IPv6 support.

    Returns:
        The localhost loopback IP.
    """
    # Try IPv4 first, then IPv6 localhost resolution
    for family in [socket.AF_INET, socket.AF_INET6]:
        try:
            dns_result = socket.getaddrinfo(
                "localhost", None, family, socket.SOCK_STREAM
            )
            return dns_result[0][4][0]
        except Exception:
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
