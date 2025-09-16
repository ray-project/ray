from typing import Optional, Tuple, Union

from ray._raylet import build_address as _build_address, parse_address as _parse_address


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


def is_localhost(host: str) -> bool:
    """Check if the given host string represents a localhost address.

    Args:
        host: The hostname or IP address to check.

    Returns:
        True if the host is a localhost address, False otherwise.
    """
    return host in ("localhost", "127.0.0.1", "::1")
