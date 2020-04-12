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
#include <sstream>
#include <string>

#include <boost/asio/generic/stream_protocol.hpp>
#ifndef _WIN32
#include <boost/asio/local/stream_protocol.hpp>
#endif
#include <boost/asio/ip/tcp.hpp>

#include "ray/util/filesystem.h"
#include "ray/util/logging.h"
#include "ray/util/util.h"

namespace ray {

/// Uses sscanf() to read a token matching from the string, advancing the iterator.
/// \param c_str A string iterator that is dereferenceable. (i.e.: c_str < string::end())
/// \param format The pattern. It must not produce any output. (e.g., use %*d, not %d.)
/// \return The scanned prefix of the string, if any.
static std::string ScanToken(std::string::const_iterator &c_str, std::string format) {
  int i = 0;
  std::string result;
  format += "%n";
  if (static_cast<size_t>(sscanf(&*c_str, format.c_str(), &i)) <= 1) {
    result.insert(result.end(), c_str, c_str + i);
    c_str += i;
  }
  return result;
}

std::string endpoint_to_url(
    const boost::asio::generic::basic_endpoint<boost::asio::generic::stream_protocol> &ep,
    bool include_scheme) {
  std::string result, scheme;
  switch (ep.protocol().family()) {
  case AF_INET: {
    scheme = "tcp://";
    boost::asio::ip::tcp::endpoint e(boost::asio::ip::tcp::v4(), 0);
    RAY_CHECK(e.size() == ep.size());
    const sockaddr *src = ep.data();
    sockaddr *dst = e.data();
    *reinterpret_cast<sockaddr_in *>(dst) = *reinterpret_cast<const sockaddr_in *>(src);
    std::ostringstream ss;
    ss << e;
    result = ss.str();
    break;
  }
  case AF_INET6: {
    scheme = "tcp://";
    boost::asio::ip::tcp::endpoint e(boost::asio::ip::tcp::v6(), 0);
    RAY_CHECK(e.size() == ep.size());
    const sockaddr *src = ep.data();
    sockaddr *dst = e.data();
    *reinterpret_cast<sockaddr_in6 *>(dst) = *reinterpret_cast<const sockaddr_in6 *>(src);
    std::ostringstream ss;
    ss << e;
    result = ss.str();
    break;
  }
  case AF_UNIX:
    scheme = "unix://";
#ifdef BOOST_ASIO_HAS_LOCAL_SOCKETS
    result.append(reinterpret_cast<const struct sockaddr_un *>(ep.data())->sun_path,
                  ep.size() - offsetof(sockaddr_un, sun_path));
#else
    RAY_LOG(FATAL) << "UNIX-domain socket endpoints are not supported";
#endif
    break;
  default:
    RAY_LOG(FATAL) << "unsupported protocol family: " << ep.protocol().family();
    break;
  }
  if (include_scheme) {
    result.insert(0, scheme);
  }
  return result;
}

boost::asio::generic::basic_endpoint<boost::asio::generic::stream_protocol>
parse_url_endpoint(const std::string &endpoint, int default_port) {
  // Syntax reference: https://en.wikipedia.org/wiki/URL#Syntax
  // Note that we're a bit more flexible, to allow parsing "127.0.0.1" as a URL.
  boost::asio::generic::stream_protocol::endpoint result;
  std::string address = endpoint, scheme;
  if (address.find("unix://") == 0) {
    scheme = "unix://";
    address.erase(0, scheme.size());
  } else if (address.size() > 0 && ray::IsDirSep(address[0])) {
    scheme = "unix://";
  } else if (address.find("tcp://") == 0) {
    scheme = "tcp://";
    address.erase(0, scheme.size());
  } else {
    scheme = "tcp://";
  }
  if (scheme == "unix://") {
#ifdef BOOST_ASIO_HAS_LOCAL_SOCKETS
    result = boost::asio::local::stream_protocol::endpoint(address);
#else
    RAY_LOG(FATAL) << "UNIX-domain socket endpoints are not supported: " << endpoint;
#endif
  } else if (scheme == "tcp://") {
    std::string::const_iterator i = address.begin();
    std::string host = ScanToken(i, "[%*[^][/]]");
    host = host.empty() ? ScanToken(i, "%*[^/:]") : host.substr(1, host.size() - 2);
    std::string port_str = ScanToken(i, ":%*d");
    int port = port_str.empty() ? default_port : std::stoi(port_str.substr(1));
    result = boost::asio::ip::tcp::endpoint(boost::asio::ip::make_address(host), port);
  } else {
    RAY_LOG(FATAL) << "Unable to parse socket endpoint: " << endpoint;
  }
  return result;
}

}  // namespace ray
