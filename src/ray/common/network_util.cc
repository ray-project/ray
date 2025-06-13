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

#include <dirent.h>
#include <unistd.h>

#include <array>
#include <cstdio>
#include <fstream>
#include <iostream>
#include <memory>
#include <regex>
#include <sstream>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

std::string GetPortStateAndPID(uint16_t port) {
  std::string cmd = "netstat -anp 2>/dev/null";
  std::array<char, 4096> buffer;
  std::string result;

  // Run netstat
  std::unique_ptr<FILE, decltype(&pclose)> pipe(popen(cmd.c_str(), "r"), pclose);
  if (!pipe) {
    result = "Failed to run netstat command.";

    return result;
  }

  // Read output
  while (fgets(buffer.data(), buffer.size(), pipe.get()) != nullptr) {
    std::string line(buffer.data());

    // Look for the port
    std::string port_str = ":" + std::to_string(port);
    if (line.find(port_str) != std::string::npos) {
      result = "Matched line: " + line;

      // Extract state and PID
      std::regex pattern(
          R"(.*:(\d+)\s+.*\s+(LISTEN|ESTABLISHED|TIME_WAIT|CLOSE_WAIT|SYN_SENT|SYN_RECV)\s+(\d+)/([^\s]+))");
      std::smatch match;
      if (std::regex_search(line, match, pattern)) {
        result += "\n  → Port: " + match[1].str();
        result += "\n  → State: " + match[2].str();
        result += "\n  → PID: " + match[3].str();
        result += "\n  → Program: " + match[4].str();
      }
    }
  }

  return result.empty() ? "Port " + std::to_string(port) + " not found." : result;
}

struct SocketEntry {
  uint16_t port;
  std::string state;
  std::string inode;
};

std::vector<SocketEntry> ParseProcNet(const std::string &path) {
  std::vector<SocketEntry> results;
  std::ifstream f(path);
  std::string line;
  std::getline(f, line);  // Skip header

  while (std::getline(f, line)) {
    std::istringstream iss(line);
    std::string sl, local_address, rem_address, state, txq, rxq, tr, tm_when, retrnsmt,
        uid, timeout, inode;
    iss >> sl >> local_address >> rem_address >> state >> txq >> rxq >> tr >> tm_when >>
        retrnsmt >> uid >> timeout >> inode;

    // Extract port
    size_t colon = local_address.find(':');
    std::string port_hex = local_address.substr(colon + 1);
    uint16_t port = std::stoul(port_hex, nullptr, 16);

    results.push_back({port, state, inode});
  }
  return results;
}

std::string FindPidByInode(const std::string &inode) {
  DIR *proc = opendir("/proc");
  if (!proc) return {};

  struct dirent *dent;
  while ((dent = readdir(proc)) != nullptr) {
    if (!isdigit(dent->d_name[0])) continue;
    std::string pid = dent->d_name;
    std::string fd_path = "/proc/" + pid + "/fd";
    DIR *fd_dir = opendir(fd_path.c_str());
    if (!fd_dir) continue;

    struct dirent *fd_entry;
    while ((fd_entry = readdir(fd_dir)) != nullptr) {
      if (fd_entry->d_type != DT_LNK) continue;
      std::string link = fd_path + "/" + fd_entry->d_name;
      char buf[1024];
      ssize_t len = readlink(link.c_str(), buf, sizeof(buf) - 1);
      if (len != -1) {
        buf[len] = '\0';
        std::string target = buf;
        if (target.find("socket:[" + inode + "]") != std::string::npos) {
          closedir(fd_dir);
          closedir(proc);
          return pid;
        }
      }
    }
    closedir(fd_dir);
  }
  closedir(proc);
  return {};
}

std::string TcpStateToString(const std::string &code) {
  static std::unordered_map<std::string, std::string> states = {{"01", "ESTABLISHED"},
                                                                {"02", "SYN_SENT"},
                                                                {"03", "SYN_RECV"},
                                                                {"04", "FIN_WAIT1"},
                                                                {"05", "FIN_WAIT2"},
                                                                {"06", "TIME_WAIT"},
                                                                {"07", "CLOSED"},
                                                                {"08", "CLOSE_WAIT"},
                                                                {"09", "LAST_ACK"},
                                                                {"0A", "LISTEN"},
                                                                {"0B", "CLOSING"}};
  auto it = states.find(code);
  return (it != states.end()) ? it->second : "UNKNOWN";
}

std::string GetPortState(uint16_t port) {
  auto entries = ParseProcNet("/proc/net/tcp");
  auto entries6 = ParseProcNet("/proc/net/tcp6");
  entries.insert(entries.end(), entries6.begin(), entries6.end());

  for (const auto &entry : entries) {
    if (entry.port == port && entry.state == "0A") {  // LISTEN
      std::string pid = FindPidByInode(entry.inode);
      if (!pid.empty()) {
        return " Port " + std::to_string(port) + " is LISTENING by PID: " + pid;
      } else {
        return " Port " + std::to_string(port) + " is LISTENING but PID not found.";
      }
    }
  }

  return "Port " + std::to_string(port) + " not found in LISTEN state.";
}

bool CheckPortFree(int port) {
  using boost::asio::ip::tcp;
  instrumented_io_context io_service;
  boost::system::error_code ec;

  tcp::acceptor acceptor(io_service);
  tcp::endpoint endpoint(tcp::v4(), port);

  acceptor.open(endpoint.protocol(), ec);
  if (ec) return false;

  // Disable reuse address explicitly
  acceptor.bind(endpoint, ec);
  if (ec) return false;

  // Listening helps ensure the port is fully bound
  acceptor.listen(boost::asio::socket_base::max_listen_connections, ec);
  if (ec) return false;

  acceptor.close();

  auto port_state = GetPortState(port);
  RAY_LOG(ERROR) << "Worker port state " << port_state << ", port: " << port;
  auto port_state_and_pid = GetPortStateAndPID(port);
  RAY_LOG(ERROR) << "Worker port state and pid " << port_state_and_pid
                 << ", port: " << port;

  return true;
}
