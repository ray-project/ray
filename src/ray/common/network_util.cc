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

std::vector<boost::asio::ip::address> GetValidLocalIpCandidates() {
  struct ifaddrs *nics_info = nullptr;
  getifaddrs(&nics_info);

  std::vector<std::pair<std::string, std::string>> nics_ips;

  struct ifaddrs *nic_info = nullptr;
  for (nic_info = nics_info; nic_info != nullptr; nic_info = nic_info->ifa_next) {

    if (nic_info->ifa_addr->sa_family==AF_INET) {
      void *addr = &((struct sockaddr_in *) nic_info->ifa_addr)->sin_addr;

      char ip[INET_ADDRSTRLEN];
      inet_ntop(AF_INET, addr, ip, INET_ADDRSTRLEN);

      nics_ips.push_back(std::make_pair(std::string(nic_info->ifa_name), std::string(ip)));
    }
  }
  freeifaddrs(nics_info);

  std::vector<boost::asio::ip::address> candidates;
  for(unsigned int i = 0; i < nics_ips.size(); ++i) {
    boost::asio::ip::address boost_addr = boost::asio::ip::make_address(nics_ips[i].second);

    if (nics_ips[i].first.find("docker") == std::string::npos
          && !boost_addr.is_loopback()) {
      candidates.push_back(boost_addr);
    }
  }

  return candidates;
}

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
      bool failed = true;

      if (!error_code) {
        std::vector<boost::asio::ip::address> ip_candidates = GetValidLocalIpCandidates();

        for (unsigned int i = 0; i < ip_candidates.size(); ++i) {
          primary_endpoint.address(ip_candidates[i]);

          AsyncClient client;
          if (client.Connect(primary_endpoint.address().to_string(), port, timeout_ms,
                             &is_timeout)) {
            failed = false;
            break;
          }
        }
      } else {
        RAY_LOG(WARNING) << "Failed to resolve ip address, error = "
                         << strerror(error_code.value());
      }

      if (!failed) {
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
