import ipaddress
from typing import List, Union


def parse_address(address: str) -> List[str]:
    """Parse a network address string into host and port.

    Args:
        address: The address string to parse (e.g., "localhost:8000", "[::1]:8000").

    Returns:
        List with [host, port] if port found, or [address] if no colon separator.
    """
    pos = address.rfind(":")
    if pos == -1:
        return [address]

    host = address[:pos]
    port = address[pos + 1 :]

    if ":" in host:
        if host.startswith("[") and host.endswith("]"):
            host = host[1:-1]
        else:
            # Invalid IPv6 (missing brackets) or colon is part of the address, not a host:port split.
            return [address]

    return [host, port]


def build_address(host: str, port: Union[int, str]) -> str:
    """Build a network address string from host and port.

    Args:
        host: The hostname or IP address.
        port: The port number (int or string).

    Returns:
        Formatted address string (e.g., "localhost:8000" or "[::1]:8000").
    """
    port_str = str(port)

    # Check if host is IPv6
    try:
        ip_addr = ipaddress.ip_address(host)
        if isinstance(ip_addr, ipaddress.IPv6Address):
            # IPv6 address
            return f"[{host}]:{port_str}"
        else:
            # IPv4 address
            return f"{host}:{port_str}"
    except ValueError:
        # hostname
        return f"{host}:{port_str}"
