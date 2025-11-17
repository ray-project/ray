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

#pragma once

#include <array>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/ip/udp.hpp>
#include <memory>
#include <optional>
#include <string>

#include "absl/container/flat_hash_map.h"

// Boost forward-declarations (to avoid forcing slow header inclusions)
namespace boost::asio::generic {

template <class Protocol>
class basic_endpoint;
class stream_protocol;

}  // namespace boost::asio::generic

namespace ray {

/// Build a network address string from host and port.
/// \param host The hostname or IP address.
/// \param port The port as a string.
/// \return Formatted address string (e.g., "localhost:8000" or "[::1]:8000").
std::string BuildAddress(const std::string &host, const std::string &port);

/// Build a network address string from host and port.
/// \param host The hostname or IP address.
/// \param port The port number.
/// \return Formatted address string (e.g., "localhost:8000" or "[::1]:8000").
std::string BuildAddress(const std::string &host, int port);

/// Parse a network address string into host and port.
/// \param address The address string to parse (e.g., "localhost:8000", "[::1]:8000").
/// \return Optional array with [host, port] if port found, nullopt if no colon separator.
std::optional<std::array<std::string, 2>> ParseAddress(const std::string &address);

/// IP address by which the local node can be reached *from* the `address`.
/// If no address is given, defaults to public DNS servers for detection.
/// \param address The IP address and port of any known live service on the
///                network you care about.
/// \return The IP address by which the local node can be reached from the address.
std::string GetNodeIpAddressFromPerspective(
    const std::optional<std::string> &address = std::nullopt);

/// Check if a host is resolved to IPv6.
/// \param host The IP or domain name to check (must be without port).
/// \return true if the host is resolved to IPv6, false if IPv4.
bool IsIPv6(const std::string &host);

/// Check whether the given port is available for the specified address family.
/// Notice, the check could be non-authentic if there're concurrent port assignments.
/// \param family The address family to check (AF_INET for IPv4, AF_INET6 for IPv6).
/// \param port The port number to check.
/// \return true if the port is available, false otherwise.
bool CheckPortFree(int family, int port);

/// Converts the given endpoint (such as TCP or UNIX domain socket address) to a string.
/// \param include_scheme Whether to include the scheme prefix (such as tcp://).
///                       This is recommended to avoid later ambiguity when parsing.
std::string EndpointToUrl(
    const boost::asio::generic::basic_endpoint<boost::asio::generic::stream_protocol> &ep,
    bool include_scheme = true);

/// Parses the endpoint socket address of a URL.
/// If a scheme:// prefix is absent, the address family is guessed automatically.
/// For TCP/IP, the endpoint comprises the IP address and port number in the URL.
/// For UNIX domain sockets, the endpoint comprises the socket path.
boost::asio::generic::basic_endpoint<boost::asio::generic::stream_protocol>
ParseUrlEndpoint(const std::string &endpoint, int default_port = 0);

/// Parse the url and return a pair of base_url and query string map.
/// EX) http://abc?num_objects=9&offset=8388878
/// will be returned as
/// {
///   url: http://abc,
///   num_objects: 9,
///   offset: 8388878
/// }
std::shared_ptr<absl::flat_hash_map<std::string, std::string>> ParseURL(std::string url);

inline bool IsLocalHost(std::string_view address, std::string_view host_address) {
  return address == "127.0.0.1" || address == "::1" || address == "localhost" ||
         address == host_address;
}

}  // namespace ray
