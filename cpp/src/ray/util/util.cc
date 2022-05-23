// Copyright 2020-2021 The Ray Authors.
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

#include "util.h"

#include <boost/algorithm/string.hpp>
#include <boost/asio.hpp>

#include "ray/util/logging.h"

namespace ray {
namespace internal {

std::string GetNodeIpAddress(const std::string &address) {
  std::vector<std::string> parts;
  boost::split(parts, address, boost::is_any_of(":"));
  RAY_CHECK(parts.size() == 2);
  try {
    boost::asio::io_service netService;
    boost::asio::ip::udp::resolver resolver(netService);
    boost::asio::ip::udp::resolver::query query(
        boost::asio::ip::udp::v4(), parts[0], parts[1]);
    boost::asio::ip::udp::resolver::iterator endpoints = resolver.resolve(query);
    boost::asio::ip::udp::endpoint ep = *endpoints;
    boost::asio::ip::udp::socket socket(netService);
    socket.connect(ep);
    boost::asio::ip::address addr = socket.local_endpoint().address();
    return addr.to_string();
  } catch (std::exception &e) {
    RAY_LOG(FATAL) << "Could not get the node IP address with socket. Exception: "
                   << e.what();
    return "";
  }
}
}  // namespace internal
}  // namespace ray
