# cython: profile=False
# distutils: language = c++
# cython: embedsignature = True
# cython: language_level = 3
# cython: c_string_encoding = default

from libcpp.string cimport string as c_string
from libcpp cimport bool as c_bool
from typing import Optional, Tuple, Union

cdef extern from *:
    """
    #include <string>
    #include "ray/util/network_util.h"

    static inline bool TryParseAddress(const std::string &address, std::string &host, std::string &port) {
      auto result = ray::ParseAddress(address);
      if (!result.has_value()) return false;
      host = (*result)[0];
      port = (*result)[1];
      return true;
    }
    """
    c_bool TryParseAddress(const c_string &address, c_string &host, c_string &port)

cdef extern from "ray/util/network_util.h" namespace "ray":
    c_string BuildAddress(const c_string &host, const c_string &port)
    c_string BuildAddress(const c_string &host, int port)


def parse_address(address: str) -> Optional[Tuple[str, str]]:
    """Parse a network address string into host and port.

    Args:
        address: The address string to parse (e.g., "localhost:8000", "[::1]:8000").

    Returns:
        Tuple with (host, port) if port found, None if no colon separator.
    """
    cdef c_string host_out
    cdef c_string port_out
    cdef c_bool success = TryParseAddress(address.encode('utf-8'), host_out, port_out)

    if not success:
        return None

    return (host_out.decode('utf-8'), port_out.decode('utf-8'))


def build_address(host: str, port: Union[int, str]) -> str:
    """Build a network address string from host and port.

    Args:
        host: The hostname or IP address.
        port: The port number (int or string).

    Returns:
        Formatted address string (e.g., "localhost:8000" or "[::1]:8000").
    """
    cdef c_string host_c = host.encode('utf-8')
    cdef c_string result
    cdef c_string port_c

    if isinstance(port, int):
        result = BuildAddress(host_c, <int>port)
    else:
        port_c = str(port).encode('utf-8')
        result = BuildAddress(host_c, port_c)

    return result.decode('utf-8')
