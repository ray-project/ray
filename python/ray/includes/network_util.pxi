from ray.includes.network_util cimport (
    BuildAddress,
    ParseAddress,
    array_string_2,
    optional,
)
from libcpp.string cimport string
from typing import Optional, Tuple, Union

def parse_address(address: str) -> Optional[Tuple[str, str]]:
    """Parse a network address string into host and port.

    Args:
        address: The address string to parse (e.g., "localhost:8000", "[::1]:8000").

    Returns:
        Tuple with (host, port) if port found, None if no colon separator.
    """
    cdef optional[array_string_2] res = ParseAddress(address.encode('utf-8'))
    if not res.has_value():
        return None

    cdef array_string_2 ip_port = res.value()
    return (ip_port[0].decode('utf-8'), ip_port[1].decode('utf-8'))


def build_address(host: str, port: Union[int, str]) -> str:
    """Build a network address string from host and port.

    Args:
        host: The hostname or IP address.
        port: The port number (int or string).

    Returns:
        Formatted address string (e.g., "localhost:8000" or "[::1]:8000").
    """
    cdef string host_c = host.encode('utf-8')
    cdef string result
    cdef string port_c

    if isinstance(port, int):
        result = BuildAddress(host_c, <int>port)
    else:
        port_c = str(port).encode('utf-8')
        result = BuildAddress(host_c, port_c)

    return result.decode('utf-8')
