from ray.includes.network_util cimport (
    BuildAddress,
    ParseAddress,
    GetNodeIpAddressFromPerspective,
    IsIPv6,
    IsLocalhostAddress,
    GetLocalhostIp,
    array_string_2,
    optional,
)
from libcpp.string cimport string
from typing import Optional, Tuple, Union
import socket

def parse_address(address: str) -> Optional[Tuple[str, str]]:
    cdef optional[array_string_2] res = ParseAddress(address.encode('utf-8'))
    if not res.has_value():
        return None

    cdef array_string_2 ip_port = res.value()
    return (ip_port[0].decode('utf-8'), ip_port[1].decode('utf-8'))


def build_address(host: str, port: Union[int, str]) -> str:
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
    cdef string host_c = host.encode('utf-8')
    return IsIPv6(host_c)


def is_localhost_address(host: str) -> bool:
    cdef string host_c = host.encode('utf-8')
    return IsLocalhostAddress(host_c)


def get_localhost_ip() -> str:
    """Get localhost loopback IP with IPv4/IPv6 support.

    Returns the localhost loopback IP ("127.0.0.1" or "::1").
    """
    cdef string result = GetLocalhostIp()
    return result.decode('utf-8')
