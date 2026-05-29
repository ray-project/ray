from ray.includes.network_util cimport (
    BuildAddress,
    ParseAddress,
    GetNodeIpAddressFromPerspective,
    IsIPv6,
    GetLocalhostIP,
    GetAllInterfacesIP,
    IsLocalhost,
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


def node_ip_address_from_perspective(address=None) -> str:
    """IP address by which the local node can be reached from the address.

    If no address is given, defaults to public DNS servers for detection.

    Args:
        address: The IP address and port of any known live service on the
            network you care about.

    Returns:
        The IP address by which the local node can be reached from the address.
    """
    cdef string node_ip
    cdef optional[string] address_c
    cdef string address_str
    if address is not None:
        address_str = address.encode('utf-8')
        address_c = optional[string](address_str)
    else:
        address_c = optional[string]()
    node_ip = GetNodeIpAddressFromPerspective(address_c)
    return node_ip.decode('utf-8')


def is_ipv6(host: str) -> bool:
    """Check if a host is resolved to IPv6.

    Args:
        host: The IP or domain name to check (must be without port).

    Returns:
        True if the host is resolved to IPv6, False if IPv4.
    """
    cdef string host_c = host.encode('utf-8')
    return IsIPv6(host_c)


def get_localhost_ip() -> str:
    """Get localhost loopback IP with IPv4/IPv6 support.

    Returns:
        "127.0.0.1" for IPv4 or "::1" for IPv6.
    """
    cdef string result = GetLocalhostIP()
    return result.decode('utf-8')


def get_all_interfaces_ip() -> str:
    """Get the IP address to bind to all network interfaces.

    Returns "0.0.0.0" for IPv4 or "::" for IPv6, depending on the system's
    localhost resolution.
    """
    cdef string result = GetAllInterfacesIP()
    return result.decode('utf-8')


def is_localhost(host: str) -> bool:
    """Check if the given host string represents a localhost address.

    Args:
        host: The hostname or IP address to check.

    Returns:
        True if the host is a localhost address (127.0.0.1, ::1, or "localhost").
    """
    cdef string host_c = host.encode('utf-8')
    return IsLocalhost(host_c)
