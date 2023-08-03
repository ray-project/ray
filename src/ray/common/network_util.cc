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
  const std::string localhost_ip = "127.0.0.1";
  bool is_timeout;

  if (async_client.Connect(kPublicDNSServerIp,
                           kPublicDNSServerPort,
                           timeout_ms,
                           &is_timeout,
                           &error_code)) {
    address = async_client.GetLocalIPAddress();
  } else {
    address = localhost_ip;

    if (is_timeout || error_code == boost::system::errc::host_unreachable) {
      boost::asio::ip::detail::endpoint primary_endpoint;
      bool success = false;

      if (!error_code) {
        std::vector<boost::asio::ip::address> ip_candidates =
            NetIf::GetValidLocalIpCandidates();

        for (const auto &ip_candidate : ip_candidates) {
          primary_endpoint.address(ip_candidate);

          AsyncClient client;
          if (client.Connect(primary_endpoint.address().to_string(),
                             port,
                             timeout_ms,
                             &is_timeout)) {
            success = true;
            break;
          }
        }
      } else {
        RAY_LOG(WARNING) << "Failed to resolve ip address, error = "
                         << strerror(error_code.value());
      }

      if (success) {
        address = primary_endpoint.address().to_string();
      }
    }
  }

  if (address == localhost_ip) {
    RAY_LOG(ERROR) << "Failed to find other valid local IP. Using " << localhost_ip
                   << ", not possible to go distributed!";
  }

  return address;
}

namespace NetIf {
std::vector<PrefixAndPriority> prefixes_and_priorities = {
    {"e", Priority::kVeryHigh},  // (Ethernet) Ex: eth0, enp7s0f1, ens160, en0

    {"w", Priority::kHigh},  // (WiFi) Ex: wlp0s20f3, wlp2s0

    {"br", Priority::kNormal},  // (Manual bridge) Ex: br0
    {"ap", Priority::kNormal},  // (Access point) Ex: ap0

    {"tun", Priority::kExclude},     // (VPN) Ex: tun0
    {"tap", Priority::kExclude},     // (VPN) Ex: tap0
    {"virbr", Priority::kExclude},   // (VM bridge) Ex: virbr0
    {"vm", Priority::kExclude},      // (VM) Ex: vmnet1
    {"vbox", Priority::kExclude},    // (VM) Ex: vboxnet1
    {"ppp", Priority::kExclude},     // (P2P) Ex: ppp0
    {"pan", Priority::kExclude},     // (Bluetooth) Ex: pan0
    {"br-", Priority::kExclude},     // (Docker bridge) Ex: br-3681c4c3d645
    {"veth", Priority::kExclude},    // (Docker virtual interface) Ex: veth7175b67
    {"docker", Priority::kExclude},  // (Docker virtual interface) Ex: docker0
};

#ifndef _WIN32

std::vector<boost::asio::ip::address> GetValidLocalIpCandidates() {
  struct ifaddrs *ifs_info = nullptr;
  getifaddrs(&ifs_info);

  std::vector<NameAndIp> ifnames_and_ips;

  struct ifaddrs *if_info = nullptr;
  for (if_info = ifs_info; if_info != nullptr; if_info = if_info->ifa_next) {
    if (if_info->ifa_addr && if_info->ifa_addr->sa_family == AF_INET) {
      void *addr = &((struct sockaddr_in *)if_info->ifa_addr)->sin_addr;

      char ip[INET_ADDRSTRLEN];
      inet_ntop(AF_INET, addr, ip, INET_ADDRSTRLEN);

      ifnames_and_ips.push_back(
          std::make_pair(std::string(if_info->ifa_name), std::string(ip)));
    }
  }
  freeifaddrs(ifs_info);

  // Bigger prefixes must be tested first in CompNameAndIps
  std::sort(
      prefixes_and_priorities.begin(), prefixes_and_priorities.end(), CompPrefixLen);

  // Filter out interfaces with small possibility of being desired to be used to serve
  std::sort(ifnames_and_ips.begin(), ifnames_and_ips.end(), CompNamesAndIps);
  while (!ifnames_and_ips.empty() &&
         GetPriority(ifnames_and_ips.back().first) == Priority::kExclude) {
    ifnames_and_ips.pop_back();
  }

  std::vector<boost::asio::ip::address> candidates;
  for (const auto &ifname_and_ip : ifnames_and_ips) {
    boost::asio::ip::address boost_addr =
        boost::asio::ip::make_address(ifname_and_ip.second);

    if (!boost_addr.is_loopback()) {
      candidates.push_back(boost_addr);
    }
  }

  return candidates;
}

#else

std::vector<boost::asio::ip::address> GetValidLocalIpCandidates() {
  boost::asio::ip::detail::endpoint primary_endpoint;
  instrumented_io_context io_context;
  boost::asio::ip::tcp::resolver resolver(io_context);
  boost::asio::ip::tcp::resolver::query query(
      boost::asio::ip::host_name(),
      "",
      boost::asio::ip::resolver_query_base::flags::v4_mapped);

  boost::asio::ip::tcp::resolver::iterator iter = resolver.resolve(query);
  boost::asio::ip::tcp::resolver::iterator end;  // End marker.

  std::vector<boost::asio::ip::address> candidates;
  for (; iter != end; iter++) {
    boost::asio::ip::tcp::endpoint ep = iter->endpoint();

    if (ep.address().is_v4() && !ep.address().is_loopback() &&
        !ep.address().is_multicast()) {
      candidates.push_back(ep.address());
    }
  }

  return candidates;
}

#endif

Priority GetPriority(const std::string &if_name) {
  for (const auto &prefix_and_priority : prefixes_and_priorities) {
    if (NameStartsWith(if_name, prefix_and_priority.first)) {
      return prefix_and_priority.second;
    }
  }

  return Priority::kExclude;
}

bool CompNamesAndIps(const NameAndIp &left, const NameAndIp &right) {
  return GetPriority(left.first) < GetPriority(right.first);
}

bool CompPrefixLen(const PrefixAndPriority &left, const PrefixAndPriority &right) {
  return left.first.size() > right.first.size();
}

bool NameStartsWith(const std::string &name, const std::string &prefix) {
  return name.compare(0, prefix.size(), prefix) == 0;
}
}  // namespace NetIf

bool CheckFree(int port) {
  instrumented_io_context io_service;
  tcp::socket socket(io_service);
  socket.open(boost::asio::ip::tcp::v4());
  boost::system::error_code ec;
  socket.bind(boost::asio::ip::tcp::endpoint(boost::asio::ip::tcp::v4(), port), ec);
  socket.close();
  return !ec.failed();
}
