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

#include "ray/util/logging.h"

std::string GetValidLocalIp(int port, int64_t timeout_ms) {
  AsyncClient async_client;
  boost::system::error_code error_code;
  std::string address;
  bool is_timeout;
  if (async_client.Connect(kPublicDNSServerIp, kPublicDNSServerPort, timeout_ms,
                           &is_timeout, &error_code)) {
    address = async_client.GetLocalIPAddress();
  } else {
    address = "127.0.0.1";

    if (is_timeout || error_code == boost::system::errc::host_unreachable) {
      boost::asio::ip::detail::endpoint primary_endpoint;
      boost::asio::io_context io_context;
      boost::asio::ip::tcp::resolver resolver(io_context);
      boost::asio::ip::tcp::resolver::query query(
          boost::asio::ip::host_name(), "",
          boost::asio::ip::resolver_query_base::flags::v4_mapped);
      boost::asio::ip::tcp::resolver::iterator iter = resolver.resolve(query, error_code);
      boost::asio::ip::tcp::resolver::iterator end;  // End marker.
      if (!error_code) {
        while (iter != end) {
          boost::asio::ip::tcp::endpoint ep = *iter;
          if (ep.address().is_v4() && !ep.address().is_loopback() &&
              !ep.address().is_multicast()) {
            primary_endpoint.address(ep.address());
            primary_endpoint.port(ep.port());

            AsyncClient client;
            if (client.Connect(primary_endpoint.address().to_string(), port, timeout_ms,
                               &is_timeout)) {
              break;
            }
          }
          iter++;
        }
      } else {
        RAY_LOG(WARNING) << "Failed to resolve ip address, error = "
                         << strerror(error_code.value());
        iter = end;
      }

      if (iter != end) {
        address = primary_endpoint.address().to_string();
      }
    }
  }

  return address;
}

bool Ping(const std::string &ip, int port, int64_t timeout_ms) {
  AsyncClient client;
  bool is_timeout;
  return client.Connect(ip, port, timeout_ms, &is_timeout);
}

bool CheckFree(int port) {
  boost::asio::io_service io_service;
  tcp::socket socket(io_service);
  socket.open(boost::asio::ip::tcp::v4());
  boost::system::error_code ec;
  socket.bind(boost::asio::ip::tcp::endpoint(boost::asio::ip::tcp::v4(), port), ec);
  socket.close();
  return !ec.failed();
}
