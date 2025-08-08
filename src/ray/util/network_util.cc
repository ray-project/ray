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
#include <optional>
#include <string>

#include "absl/strings/str_format.h"

using boost::asio::ip::tcp;

namespace ray {

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

bool CheckPortFree(int port) {
  boost::asio::io_context io_service;
  tcp::socket socket(io_service);
  socket.open(tcp::v4());
  boost::system::error_code ec;
  socket.bind(tcp::endpoint(tcp::v4(), port), ec);
  socket.close();
  return !ec.failed();
}

std::string GetLoopbackAddress() {
  static std::string cached_address = []() -> std::string {
    // First try IPv4
    boost::asio::io_context io_context;
    tcp::socket socket(io_context);
    boost::system::error_code ec;

    socket.open(tcp::v4(), ec);
    if (!ec) {
      socket.bind(tcp::endpoint(boost::asio::ip::address_v4::loopback(), 0), ec);
      socket.close();
      if (!ec) {
        return "127.0.0.1";
      }
    }

    // Try IPv6 if IPv4 failed
    socket.open(tcp::v6(), ec);
    if (!ec) {
      socket.bind(tcp::endpoint(boost::asio::ip::address_v6::loopback(), 0), ec);
      socket.close();
      if (!ec) {
        return "::1";
      }
    }

    // If neither works, fallback to IPv4
    return "127.0.0.1";
  }();

  return cached_address;
}

std::string GetBindAllAddress() {
  static std::string cached_address = []() -> std::string {
    // First try IPv4
    boost::asio::io_context io_context;
    tcp::socket socket(io_context);
    boost::system::error_code ec;

    socket.open(tcp::v4(), ec);
    if (!ec) {
      socket.bind(tcp::endpoint(boost::asio::ip::address_v4::any(), 0), ec);
      socket.close();
      if (!ec) {
        return "0.0.0.0";
      }
    }

    // Try IPv6 if IPv4 failed
    socket.open(tcp::v6(), ec);
    if (!ec) {
      socket.bind(tcp::endpoint(boost::asio::ip::address_v6::any(), 0), ec);
      socket.close();
      if (!ec) {
        return "::";
      }
    }

    // If neither works, fallback to IPv4
    return "0.0.0.0";
  }();

  return cached_address;
}

}  // namespace ray
