// Copyright 2020 The Ray Authors.
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

#include "ray/util/util.h"

#include <stdio.h>
#include <stdlib.h>
#ifndef _WIN32
#include <sys/un.h>
#endif

#include <algorithm>
#include <boost/asio/generic/stream_protocol.hpp>
#include <memory>
#include <sstream>
#include <string>
#include <string_view>
#include <utility>
#include <vector>
#ifndef _WIN32
#include <boost/asio/local/stream_protocol.hpp>
#endif
#include <boost/asio/ip/tcp.hpp>

#include "absl/strings/match.h"
#include "ray/util/filesystem.h"
#include "ray/util/logging.h"
#include "ray/util/process.h"

std::string EndpointToUrl(
    const boost::asio::generic::basic_endpoint<boost::asio::generic::stream_protocol> &ep,
    bool include_scheme) {
  std::string result, scheme;
  switch (ep.protocol().family()) {
  case AF_INET: {
    scheme = "tcp://";
    boost::asio::ip::tcp::endpoint e(boost::asio::ip::tcp::v4(), 0);
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
    boost::asio::ip::tcp::endpoint e(boost::asio::ip::tcp::v6(), 0);
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
    std::string host = ::ray::ScanToken(i, "[%*[^][/]]");
    host =
        host.empty() ? ::ray::ScanToken(i, "%*[^/:]") : host.substr(1, host.size() - 2);
    std::string port_str = ::ray::ScanToken(i, ":%*d");
    int port = port_str.empty() ? default_port : std::stoi(port_str.substr(1));
    result = boost::asio::ip::tcp::endpoint(boost::asio::ip::make_address(host), port);
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

std::string GenerateUUIDV4() {
  thread_local std::random_device rd;
  thread_local std::mt19937 gen(rd());
  std::uniform_int_distribution<> dis(0, 15);
  std::uniform_int_distribution<> dis2(8, 11);

  std::stringstream ss;
  int i;
  ss << std::hex;
  for (i = 0; i < 8; i++) {
    ss << dis(gen);
  }
  ss << "-";
  for (i = 0; i < 4; i++) {
    ss << dis(gen);
  }
  ss << "-4";
  for (i = 0; i < 3; i++) {
    ss << dis(gen);
  }
  ss << "-";
  ss << dis2(gen);
  for (i = 0; i < 3; i++) {
    ss << dis(gen);
  }
  ss << "-";
  for (i = 0; i < 12; i++) {
    ss << dis(gen);
  };
  return ss.str();
}

namespace ray {

bool IsRayletFailed(const std::string &raylet_pid) {
  auto should_shutdown = false;
  if (!raylet_pid.empty()) {
    auto pid = static_cast<pid_t>(std::stoi(raylet_pid));
    if (!IsProcessAlive(pid)) {
      should_shutdown = true;
    }
  } else if (!IsParentProcessAlive()) {
    should_shutdown = true;
  }
  return should_shutdown;
}

void QuickExit() {
  ray::RayLog::ShutDownRayLog();
  _Exit(1);
}

std::optional<std::chrono::steady_clock::time_point> ToTimeoutPoint(int64_t timeout_ms) {
  std::optional<std::chrono::steady_clock::time_point> timeout_point;
  if (timeout_ms == -1) {
    return timeout_point;
  }
  auto now = std::chrono::steady_clock::now();
  auto timeout_duration = std::chrono::milliseconds(timeout_ms);
  timeout_point.emplace(now + timeout_duration);
  return timeout_point;
}

}  // namespace ray
