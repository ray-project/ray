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

typedef std::pair<std::string, std::string> NicAndIp;

bool StartsWith(std::string s, std::string start) {
  return s.find(start) == 0;
}

bool CompNicsAndIps(NicAndIp a, NicAndIp b){
  return GetNicPriority(a.first) < GetNicPriority(b.first);
}

int GetNicPriority(std::string nic_name){
  int priority = PRIORITY_TO_DELETE;  // smaller number, more priority

  if(StartsWith(nic_name, "e")){  // eth0, enp7s0f1, ens160, en0, etc
    priority = 1;
  }
  else if (StartsWith(nic_name, "w")) {  // wlp0s20f3, wlp2s0, etc
    priority = 10;
  }
  else if (StartsWith(nic_name, "br")) {
    if (nic_name[2] == '-') {
      // Probably docker bridge
      priority = 100;  // br-3681c4c3d645, etc
    } else {
      priority = 20;  // br0, etc
    }
  }
  else if (StartsWith(nic_name, "ap")) {  // ap0, etc
    // Probably Access Point
    priority = 40;
  }
  else if (StartsWith(nic_name, "tun") || StartsWith(nic_name, "tap")) {  // tap0, tun0, etc
    // Probably VPN
    priority = 60;
  }
  else if (StartsWith(nic_name, "virbr")  // virbr0, etc
      || StartsWith(nic_name, "vm")  // vmnet1, etc
      || StartsWith(nic_name, "vbox")) {  // vboxnet1, etc
    priority = 70;
  }
  else if (StartsWith(nic_name, "ppp")) {  // ppp0, etc
    // Point to Point control: 3g, modem, etc
    priority = 80;  
  }
  else if (StartsWith(nic_name, "pan")) {  // pan0, pan1, etc
    // Bluetooth
    priority = 90;  
  }
  else if (StartsWith(nic_name, "veth")) {  // veth7175b67, etc
    // Probably docker
    priority = 110;
  }
  else if (StartsWith(nic_name, "docker")) {  // docker0, etc
    // Probably docker
    priority = 120;
  }

  return priority;
}

#ifndef _WIN32

std::vector<boost::asio::ip::address> GetValidLocalIpCandidates() {
  struct ifaddrs *nics_info = nullptr;
  getifaddrs(&nics_info);

  std::vector<NicAndIp> nics_and_ips;

  struct ifaddrs *nic_info = nullptr;
  for (nic_info = nics_info; nic_info != nullptr; nic_info = nic_info->ifa_next) {

    if (nic_info->ifa_addr->sa_family == AF_INET) {
      void *addr = &((struct sockaddr_in *) nic_info->ifa_addr)->sin_addr;

      char ip[INET_ADDRSTRLEN];
      inet_ntop(AF_INET, addr, ip, INET_ADDRSTRLEN);

      nics_and_ips.push_back(std::make_pair(std::string(nic_info->ifa_name), std::string(ip)));
    }
  }
  freeifaddrs(nics_info);

  // Filter out NICs with small possibility of being desired to be used to serve
  std::sort(nics_and_ips.begin(), nics_and_ips.end(), CompNicsAndIps);
  while (GetNicPriority(nics_and_ips.back().first) > PRIORITY_TO_DELETE) {
    nics_and_ips.pop_back();
  }

  std::vector<boost::asio::ip::address> candidates;
  for(unsigned int i = 0; i < nics_and_ips.size(); ++i) {
    boost::asio::ip::address boost_addr = boost::asio::ip::make_address(nics_and_ips[i].second);

    if (!boost_addr.is_loopback()) {
      candidates.push_back(boost_addr);
    }
  }

  return candidates;
}

#else

std::vector<boost::asio::ip::address> GetValidLocalIpCandidates() {
  boost::asio::ip::detail::endpoint primary_endpoint;
  boost::asio::io_context io_context;
  boost::asio::ip::tcp::resolver resolver(io_context);
  boost::asio::ip::tcp::resolver::query query(
      boost::asio::ip::host_name(), "",
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
