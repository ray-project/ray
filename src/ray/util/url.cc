// Copyright 2017 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
