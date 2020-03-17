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

#ifdef _WIN32
#include <afunix.h>
#else
#include <sys/un.h>
#endif

#include <algorithm>
#include <boost/asio/generic/stream_protocol.hpp>
#ifndef _WIN32
#include <boost/asio/local/stream_protocol.hpp>
#endif
#include <boost/asio/ip/tcp.hpp>
#include <sstream>
#include <string>

#include "ray/util/filesystem.h"
#include "ray/util/logging.h"
#include "ray/util/util.h"

namespace ray {

std::string endpoint_to_url(
    const boost::asio::generic::basic_endpoint<boost::asio::generic::stream_protocol> &ep,
    bool include_scheme) {
  std::string result;
  switch (ep.protocol().family()) {
  case AF_INET:
  case AF_INET6: {
    boost::asio::ip::tcp protocol = ep.protocol().family() == AF_INET
                                        ? boost::asio::ip::tcp::v4()
                                        : boost::asio::ip::tcp::v6();
    boost::asio::ip::tcp::endpoint e(protocol, 0);
    RAY_DCHECK(e.size() == ep.size());
    memcpy(e.data(), ep.data(), ep.size());
    std::ostringstream ss;
    ss << e;
    result = ss.str();
    if (include_scheme) {
      result.insert(0, "tcp://");
    }
    break;
  }
  case AF_UNIX: {
    size_t path_offset = offsetof(sockaddr_un, sun_path);
    result.append(reinterpret_cast<const char *>(ep.data()) + path_offset,
                  ep.size() - path_offset);
    if (include_scheme) {
      result.insert(0, "unix://");
    }
    break;
  }
  default:
    RAY_LOG(FATAL) << "unsupported protocol family: " << ep.protocol().family();
    break;
  }
  return result;
}

boost::asio::generic::basic_endpoint<boost::asio::generic::stream_protocol>
parse_url_endpoint(const std::string &endpoint, int default_port) {
  // Syntax reference: https://en.wikipedia.org/wiki/URL#Syntax
  // Note that we're a bit more flexible, to allow parsing "127.0.0.1" as a URL.
  boost::asio::generic::stream_protocol::endpoint result;
  const std::string ep_plus_null = endpoint + '\0';  // so we can dereference a '\0'
  std::string::const_iterator i = ep_plus_null.begin(), j = ep_plus_null.end() - 1;
  std::string scheme = ScanToken(i, "%*1[A-Za-z]%*[-+.A-Za-z0-9]:");
  ScanToken(i, "//");
  if (!scheme.empty()) {
    scheme.pop_back();  // remove colon
  }
  if (scheme.empty()) {
    if (ray::IsDirSep(*i) || *i == '.' || *i == '\0') {
      scheme = "unix";
    } else {
      scheme = "tcp";
    }
  }
  if (scheme == "unix") {
    std::string rest(i, j);
#ifdef _WIN32
    sockaddr_un sa = {};
    sa.sun_family = AF_UNIX;
    size_t k = offsetof(sockaddr_un, sun_path);
    std::string addr = std::string(reinterpret_cast<const char *>(&sa), k) + rest;
    result = boost::asio::generic::stream_protocol::endpoint(addr.c_str(), addr.size());
#else
    result = boost::asio::local::stream_protocol::endpoint(rest);
#endif
  }
  if (scheme == "tcp") {
    std::string host = ScanToken(i, "[%*[^][/]]");
    host = host.empty() ? ScanToken(i, "%*[^/:]") : host.substr(1, host.size() - 2);
    std::string port_str = ScanToken(i, ":%*d");
    int port = port_str.empty() ? default_port : atoi(port_str.c_str() + 1);
    result = boost::asio::ip::tcp::endpoint(boost::asio::ip::make_address(host), port);
  }
  return result;
}

}  // namespace ray
