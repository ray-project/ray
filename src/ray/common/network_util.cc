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

#include "ray/common/network_util.h"

#include <algorithm>
#include <boost/asio/ip/address.hpp>
#include <cctype>

#include "absl/strings/str_format.h"
#include "ray/common/asio/instrumented_io_context.h"
#include "ray/util/logging.h"

using boost::asio::ip::tcp;

bool CheckPortFree(int port) {
  instrumented_io_context io_service;
  tcp::socket socket(io_service);
  socket.open(boost::asio::ip::tcp::v4());
  boost::system::error_code ec;
  socket.bind(boost::asio::ip::tcp::endpoint(boost::asio::ip::tcp::v4(), port), ec);
  socket.close();
  return !ec.failed();
}

std::string BuildAddress(const std::string &host, int port) {
  boost::system::error_code ec;
  auto ip_addr = boost::asio::ip::make_address(host, ec);
  if (!ec && ip_addr.is_v6()) {
    // IPv6 address
    return absl::StrFormat("[%s]:%d", host, port);
  } else {
    // IPv4 address or hostname
    return absl::StrFormat("%s:%d", host, port);
  }
}

std::string BuildAddress(const std::string &host, const std::string &port) {
  boost::system::error_code ec;
  auto ip_addr = boost::asio::ip::make_address(host, ec);
  if (!ec && ip_addr.is_v6()) {
    // IPv6 address
    return absl::StrFormat("[%s]:%s", host, port);
  } else {
    // IPv4 address or hostname
    return absl::StrFormat("%s:%s", host, port);
  }
}

std::vector<std::string> ParseAddress(const std::string &address) {
  size_t pos = address.find_last_of(":");
  if (pos == std::string::npos) {
    return {address};
  }

  std::string host = address.substr(0, pos);
  std::string port = address.substr(pos + 1);

  // Non-numeric port indicates an invalid host:port split — interpret as a single host
  // with no port.
  if (!port.empty() && !std::all_of(port.begin(), port.end(), ::isdigit)) {
    return {address};
  }

  // IPv6 address
  if (host.size() >= 2 && host.front() == '[' && host.back() == ']') {
    host = host.substr(1, host.size() - 2);
  }

  return {host, port};
}
