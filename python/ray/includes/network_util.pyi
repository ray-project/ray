# source: network_util.pyi
from typing import Optional, Tuple, Union

def parse_address(address: str) -> Optional[Tuple[str, str]]:
    """Parse a network address string into host and port.

    Args:
        address: The address string to parse (e.g., "localhost:8000", "[::1]:8000").

    Returns:
        Tuple with (host, port) if port found, None if no colon separator.
    """
    ...

def build_address(host: str, port: Union[int, str]) -> str:
    """Build a network address string from host and port.

    Args:
        host: The hostname or IP address.
        port: The port number (int or string).

    Returns:
        Formatted address string (e.g., "localhost:8000" or "[::1]:8000").
    """
    ...
