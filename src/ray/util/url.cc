#include "ray/util/url.h"

#include <stdlib.h>

#include <algorithm>
#include <boost/asio/ip/address.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <string>

#include "ray/util/logging.h"

boost::asio::ip::tcp::endpoint parse_ip_tcp_endpoint(const std::string &endpoint,
                                                     int default_port) {
  const std::string scheme_sep = "://";
  size_t scheme_begin = 0, scheme_end = endpoint.find(scheme_sep, scheme_begin);
  size_t host_begin;
  if (scheme_end < endpoint.size()) {
    host_begin = scheme_end + scheme_sep.size();
  } else {
    scheme_end = scheme_begin;
    host_begin = scheme_end;
  }
  std::string scheme = endpoint.substr(scheme_begin, scheme_end - scheme_begin);
  RAY_CHECK(scheme_end == host_begin || scheme == "tcp");
  size_t port_end = endpoint.find('/', host_begin);
  if (port_end >= endpoint.size()) {
    port_end = endpoint.size();
  }
  size_t host_end, port_begin;
  if (endpoint.find('[', host_begin) == host_begin) {
    // IPv6 with brackets (optional ports)
    ++host_begin;
    host_end = endpoint.find(']', host_begin);
    if (host_end < port_end) {
      port_begin = endpoint.find(':', host_end + 1);
      if (port_begin < port_end) {
        ++port_begin;
      } else {
        port_begin = port_end;
      }
    } else {
      host_end = port_end;
      port_begin = host_end;
    }
  } else if (std::count(endpoint.begin() + static_cast<ptrdiff_t>(host_begin),
                        endpoint.begin() + static_cast<ptrdiff_t>(port_end), ':') > 1) {
    // IPv6 without brackets (no ports)
    port_begin = port_end;
    host_end = port_begin;
  } else {
    // IPv4
    host_end = endpoint.find(':', host_begin);
    if (host_end < port_end) {
      port_begin = host_end + 1;
    } else {
      host_end = port_end;
      port_begin = host_end;
    }
  }
  std::string host = endpoint.substr(host_begin, host_end - host_begin);
  std::string port_str = endpoint.substr(port_begin, port_end - port_begin);
  boost::asio::ip::address address = boost::asio::ip::make_address(host);
  int port = port_str.empty() ? default_port : atoi(port_str.c_str());
  return boost::asio::ip::tcp::endpoint(address, port);
}
