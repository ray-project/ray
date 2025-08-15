from typing import Optional, Tuple, Union


def parse_address(address: str) -> Optional[Tuple[str, str]]:
    """Parse a network address string into host and port.

    Args:
        address: The address string to parse (e.g., "localhost:8000", "[::1]:8000").

    Returns:
        Tuple with (host, port) if port found, None if no colon separator.
    """
    pos = address.rfind(":")
    if pos == -1:
        return None

    host = address[:pos]
    port = address[pos + 1 :]

    if ":" in host:
        if host.startswith("[") and host.endswith("]"):
            host = host[1:-1]
        else:
            # Invalid IPv6 (missing brackets) or colon is part of the address, not a host:port split.
            return None

    return (host, port)


def build_address(host: str, port: Union[int, str]) -> str:
    """Build a network address string from host and port.

    Args:
        host: The hostname or IP address.
        port: The port number (int or string).

    Returns:
        Formatted address string (e.g., "localhost:8000" or "[::1]:8000").
    """
    if host is not None and ":" in host:
        # IPv6 address
        return f"[{host}]:{port}"
    # IPv4 address or hostname
    return f"{host}:{port}"
