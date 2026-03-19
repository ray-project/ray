from libc.stddef cimport size_t
from libcpp.string cimport string
from libcpp cimport bool
from ray.includes.array cimport array_string_2
from ray.includes.optional cimport optional

cdef extern from "ray/util/network_util.h" namespace "ray":
    string BuildAddress(const string &host, int port)
    string BuildAddress(const string &host, const string &port)
    optional[array_string_2] ParseAddress(const string &address)
    string GetNodeIpAddressFromPerspective(const optional[string] &address)
    bool IsIPv6(const string &host)
