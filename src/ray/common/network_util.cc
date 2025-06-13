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

#include "ray/common/asio/instrumented_io_context.h"
#include "ray/util/logging.h"

using boost::asio::ip::tcp;

#include <fstream>
#include <iomanip>
#include <iostream>
#include <optional>
#include <regex>
#include <sstream>
#include <unordered_map>

enum class TcpState {
  ESTABLISHED,
  SYN_SENT,
  SYN_RECV,
  FIN_WAIT1,
  FIN_WAIT2,
  TIME_WAIT,
  CLOSED,
  CLOSE_WAIT,
  LAST_ACK,
  LISTEN,
  CLOSING,  // normal states
  UNKNOWN   // not in table / unmapped
};

struct Entry {
  uint32_t local_port;
  TcpState state;
};

// Hex‑code → enum map exactly as the kernel prints it.
static const std::unordered_map<std::string, TcpState> kStateMap{
    {"01", TcpState::ESTABLISHED},
    {"02", TcpState::SYN_SENT},
    {"03", TcpState::SYN_RECV},
    {"04", TcpState::FIN_WAIT1},
    {"05", TcpState::FIN_WAIT2},
    {"06", TcpState::TIME_WAIT},
    {"07", TcpState::CLOSED},
    {"08", TcpState::CLOSE_WAIT},
    {"09", TcpState::LAST_ACK},
    {"0A", TcpState::LISTEN},
    {"0B", TcpState::CLOSING},
};

// Reads one of the proc files and appends every row into *out.
void ParseProcFile(const std::string &path, std::vector<Entry> &out) {
  std::ifstream f(path);
  std::string line;
  std::getline(f, line);  // skip header

  while (std::getline(f, line)) {
    std::istringstream iss(line);
    std::string sl, local, remote, st;
    iss >> sl >> local >> remote >> st;  // we only need these three

    // local is "0100007F:138B" → split on ':' and keep port part.
    auto pos = local.find(':');
    if (pos == std::string::npos) continue;
    std::string port_hex = local.substr(pos + 1);

    unsigned port;
    std::stringstream ss;
    ss << std::hex << port_hex;
    ss >> port;

    auto it = kStateMap.find(st);
    TcpState state = (it == kStateMap.end()) ? TcpState::UNKNOWN : it->second;
    out.push_back({static_cast<uint32_t>(port), state});
  }
}

// Convenience → human‑readable string.
std::string ToString(TcpState s) {
  switch (s) {
  case TcpState::ESTABLISHED:
    return "ESTABLISHED";
  case TcpState::SYN_SENT:
    return "SYN_SENT";
  case TcpState::SYN_RECV:
    return "SYN_RECV";
  case TcpState::FIN_WAIT1:
    return "FIN_WAIT1";
  case TcpState::FIN_WAIT2:
    return "FIN_WAIT2";
  case TcpState::TIME_WAIT:
    return "TIME_WAIT";
  case TcpState::CLOSED:
    return "CLOSED";
  case TcpState::CLOSE_WAIT:
    return "CLOSE_WAIT";
  case TcpState::LAST_ACK:
    return "LAST_ACK";
  case TcpState::LISTEN:
    return "LISTEN";
  case TcpState::CLOSING:
    return "CLOSING";
  default:
    return "UNKNOWN";
  }
}

// Return the first state we find for *exactly* this port, or std::nullopt if the
// port is missing from the kernel tables (meaning no socket at all).
std::string GetPortState(uint16_t port) {
  std::vector<Entry> entries;
  ParseProcFile("/proc/net/tcp", entries);
  ParseProcFile("/proc/net/tcp6", entries);

  for (const auto &e : entries) {
    if (e.local_port == port) {
      return ToString(e.state);
    }
  }
  return "unknown";
}

bool CheckPortFree(int port) {
  instrumented_io_context io_service;
  tcp::socket socket(io_service);
  socket.open(boost::asio::ip::tcp::v4());

  // Disable SO_REUSEADDR
  boost::asio::socket_base::reuse_address option(false);
  socket.set_option(option);

  boost::system::error_code ec;
  socket.bind(boost::asio::ip::tcp::endpoint(boost::asio::ip::tcp::v4(), port), ec);
  socket.close();
  return !ec.failed();
}
