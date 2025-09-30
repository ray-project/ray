// Copyright 2025 The Ray Authors.
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

#include "ray/util/network_util.h"

#include <array>
#include <boost/asio.hpp>
#include <boost/asio/generic/stream_protocol.hpp>
#ifndef _WIN32
#include <errno.h>
#include <netinet/in.h>
#include <sys/socket.h>

#include <boost/asio/local/stream_protocol.hpp>
#else
#include <winsock2.h>
#include <ws2tcpip.h>
#endif
#include <boost/asio/ip/tcp.hpp>
#include <cstdlib>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "absl/strings/match.h"
#include "absl/strings/str_format.h"
#include "absl/strings/str_join.h"
#include "ray/util/filesystem.h"
#include "ray/util/logging.h"
#include "ray/util/string_utils.h"

using boost::asio::io_context;
using boost::asio::ip::tcp;

namespace ray {

bool IsIPv6(const std::string &host) {
  boost::system::error_code ec;
  auto addr = boost::asio::ip::make_address(host, ec);
  if (!ec) {
    return addr.is_v6();
  }

  // host is domain name.
  boost::asio::io_service io_service;
  boost::asio::ip::tcp::resolver resolver(io_service);

  // try IPv4 first, then IPv6 resolution
  boost::system::error_code ec_v4;
  auto results_v4 = resolver.resolve(boost::asio::ip::tcp::v4(), host, "0", ec_v4);
  if (!ec_v4 && !results_v4.empty()) {
    return false;
  }

  boost::system::error_code ec_v6;
  auto results_v6 = resolver.resolve(boost::asio::ip::tcp::v6(), host, "0", ec_v6);
  if (!ec_v6 && !results_v6.empty()) {
    return true;
  }

  RAY_LOG(WARNING) << "Failed to resolve hostname '" << host
                   << "': IPv4 error: " << ec_v4.message()
                   << ", IPv6 error: " << ec_v6.message();
  return false;
}

std::string BuildAddress(const std::string &host, const std::string &port) {
  if (host.find(':') != std::string::npos) {
    // IPv6 address
    return absl::StrFormat("[%s]:%s", host, port);
  } else {
    // IPv4 address or hostname
    return absl::StrFormat("%s:%s", host, port);
  }
}

std::string BuildAddress(const std::string &host, int port) {
  return BuildAddress(host, std::to_string(port));
}

std::optional<std::array<std::string, 2>> ParseAddress(const std::string &address) {
  size_t pos = address.find_last_of(":");
  if (pos == std::string::npos) {
    return std::nullopt;
  }

  std::string host = address.substr(0, pos);
  std::string port = address.substr(pos + 1);

  if (host.find(':') != std::string::npos) {
    if (host.size() >= 2 && host.front() == '[' && host.back() == ']') {
      host = host.substr(1, host.size() - 2);
    } else {
      // Invalid IPv6 (missing brackets) or colon is part of the address, not a host:port
      // split.
      return std::nullopt;
    }
  }

  return std::array<std::string, 2>{host, port};
}

bool CheckPortFree(int family, int port) {
  io_context io_service;

  std::unique_ptr<boost::asio::ip::tcp::socket> socket;
  boost::system::error_code ec;

  if (family == AF_INET6) {
    socket = std::make_unique<boost::asio::ip::tcp::socket>(io_service,
                                                            boost::asio::ip::tcp::v6());
    socket->bind(tcp::endpoint(tcp::v6(), port), ec);
  } else {
    socket = std::make_unique<boost::asio::ip::tcp::socket>(io_service,
                                                            boost::asio::ip::tcp::v4());
    socket->bind(tcp::endpoint(tcp::v4(), port), ec);
  }

  socket->close();
  return !ec.failed();
}

std::string EndpointToUrl(
    const boost::asio::generic::basic_endpoint<boost::asio::generic::stream_protocol> &ep,
    bool include_scheme) {
  std::string result, scheme;
  switch (ep.protocol().family()) {
  case AF_INET: {
    scheme = "tcp://";
    tcp::endpoint e(tcp::v4(), 0);
    RAY_CHECK_EQ(e.size(), ep.size());
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
    tcp::endpoint e(tcp::v6(), 0);
    RAY_CHECK_EQ(e.size(), ep.size());
    const sockaddr *src = ep.data();
    sockaddr *dst = e.data();
    *reinterpret_cast<sockaddr_in6 *>(dst) = *reinterpret_cast<const sockaddr_in6 *>(src);
    std::ostringstream ss;
    ss << e;
    result = ss.str();
    break;
  }
#if defined(BOOST_ASIO_HAS_LOCAL_SOCKETS) && !defined(_WIN32)
  case AF_UNIX:
    scheme = "unix://";
    result.append(reinterpret_cast<const struct sockaddr_un *>(ep.data())->sun_path,
                  ep.size() - offsetof(sockaddr_un, sun_path));
    break;
#endif
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
ParseUrlEndpoint(const std::string &endpoint, int default_port) {
  // Syntax reference: https://en.wikipedia.org/wiki/URL#Syntax
  // Note that we're a bit more flexible, to allow parsing "127.0.0.1" as a URL.
  boost::asio::generic::stream_protocol::endpoint result;
  std::string address = endpoint, scheme;
  if (absl::StartsWith(address, "unix://")) {
    scheme = "unix://";
    address.erase(0, scheme.size());
  } else if (!address.empty() && ray::IsDirSep(address[0])) {
    scheme = "unix://";
  } else if (absl::StartsWith(address, "tcp://")) {
    scheme = "tcp://";
    address.erase(0, scheme.size());
  } else {
    scheme = "tcp://";
  }
  if (scheme == "unix://") {
#if defined(BOOST_ASIO_HAS_LOCAL_SOCKETS) && !defined(_WIN32)
    size_t maxlen = sizeof(sockaddr_un().sun_path) / sizeof(*sockaddr_un().sun_path) - 1;
    RAY_CHECK(address.size() <= maxlen)
        << "AF_UNIX path length cannot exceed " << maxlen << " bytes: " << address;
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
    result = tcp::endpoint(boost::asio::ip::make_address(host), port);
  } else {
    RAY_LOG(FATAL) << "Unable to parse socket endpoint: " << endpoint;
  }
  return result;
}

std::shared_ptr<absl::flat_hash_map<std::string, std::string>> ParseURL(std::string url) {
  auto result = std::make_shared<absl::flat_hash_map<std::string, std::string>>();
  std::string delimiter = "?";
  size_t pos = 0;
  pos = url.find(delimiter);
  if (pos == std::string::npos) {
    return result;
  }

  const std::string base_url = url.substr(0, pos);
  result->emplace("url", base_url);
  url.erase(0, pos + delimiter.length());
  const std::string query_delimeter = "&";

  auto parse_key_value_with_equal_delimter =
      [](std::string_view key_value) -> std::pair<std::string_view, std::string_view> {
    // Parse the query key value pair.
    const std::string key_value_delimter = "=";
    size_t key_value_pos = key_value.find(key_value_delimter);
    std::string_view key = key_value.substr(0, key_value_pos);
    return std::make_pair(key, key_value.substr(key.size() + 1));
  };

  while ((pos = url.find(query_delimeter)) != std::string::npos) {
    std::string_view token = std::string_view{url}.substr(0, pos);
    auto key_value_pair = parse_key_value_with_equal_delimter(token);
    result->emplace(std::string(key_value_pair.first),
                    std::string(key_value_pair.second));
    url.erase(0, pos + delimiter.length());
  }
  std::string_view token = std::string_view{url}.substr(0, pos);
  auto key_value_pair = parse_key_value_with_equal_delimter(token);
  result->emplace(std::string(key_value_pair.first), std::string(key_value_pair.second));
  return result;
}

std::string GetNodeIpAddressFromPerspective(const std::optional<std::string> &address) {
  std::vector<std::pair<std::string, boost::asio::ip::udp>> test_addresses;
  if (address.has_value()) {
    auto parts = ParseAddress(*address);
    if (parts.has_value()) {
      if (IsIPv6((*parts)[0])) {
        test_addresses = {{*address, boost::asio::ip::udp::v6()}};
      } else {
        test_addresses = {{*address, boost::asio::ip::udp::v4()}};
      }
    }
  } else {
    test_addresses = {{"8.8.8.8:53", boost::asio::ip::udp::v4()},
                      {"[2001:4860:4860::8888]:53", boost::asio::ip::udp::v6()}};
  }

  // Try socket-based detection with IPv4/IPv6
  std::vector<std::string> failed_addresses;
  for (const auto &[addr_str, protocol] : test_addresses) {
    auto parts = ParseAddress(addr_str);
    if (!parts.has_value()) continue;

    try {
      boost::asio::io_service net_service;
      boost::asio::ip::udp::resolver resolver(net_service);
      boost::asio::ip::udp::resolver::query query(protocol, (*parts)[0], (*parts)[1]);
      auto endpoints = resolver.resolve(query);
      boost::asio::ip::udp::endpoint ep = *endpoints;
      boost::asio::ip::udp::socket socket(net_service, protocol);
      socket.connect(ep);
      boost::asio::ip::address local_addr = socket.local_endpoint().address();
      return local_addr.to_string();
    } catch (const std::exception &ex) {
      // Continue to next address/protocol combination
      failed_addresses.push_back(addr_str);
      continue;
    }
  }

  RAY_LOG(WARNING) << "Failed to determine local IP via external connectivity to: "
                   << absl::StrJoin(failed_addresses, ", ")
                   << ", falling back to hostname resolution";
  try {
    boost::asio::io_service net_service;
    boost::asio::ip::tcp::resolver resolver(net_service);
    boost::asio::ip::tcp::resolver::query query(boost::asio::ip::host_name(), "");
    auto endpoints = resolver.resolve(query);

    std::string ipv6_candidate;
    for (const auto &endpoint : endpoints) {
      if (endpoint.endpoint().address().is_v4()) {
        return endpoint.endpoint().address().to_string();
      } else if (endpoint.endpoint().address().is_v6() && ipv6_candidate.empty()) {
        ipv6_candidate = endpoint.endpoint().address().to_string();
      }
    }

    if (!ipv6_candidate.empty()) {
      return ipv6_candidate;
    }
  } catch (const std::exception &ex) {
    // Hostname resolution failed
    RAY_LOG(WARNING) << "Hostname resolution failed: " << ex.what();
  }

  // Final fallback
  RAY_LOG(WARNING) << "Unable to detect local IP address. Defaulting to 127.0.0.1";
  return "127.0.0.1";
}

}  // namespace ray
