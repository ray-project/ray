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

#pragma once

#include <chrono>
#include <filesystem>
#include <fstream>
#include <string>
#include <thread>

#include "nlohmann/json.hpp"
#include "ray/common/id.h"
#include "ray/common/status.h"
#include "ray/common/status_or.h"

namespace ray {

/// Write a key-value pair to a file atomically.
/// Uses temp file + rename for cross-platform(linux/windows and
/// cross-language(C++/Python) safe file sharing.
inline Status WriteKVFile(const std::string &file_path,
                          const std::string &key,
                          const std::string &value) {
  std::string tmp_path = file_path + ".tmp";
  std::ofstream file(tmp_path);
  if (!file.is_open()) {
    return Status::IOError("Failed to open temp file for writing: " + tmp_path);
  }
  nlohmann::json j;
  j[key] = value;
  file << j;
  file.close();
  if (file.fail()) {
    return Status::IOError("Failed to write to temp file: " + tmp_path);
  }

  std::error_code ec;
  // On Windows, rename doesn't overwrite existing files, so remove first.
  std::filesystem::remove(file_path, ec);
  std::filesystem::rename(tmp_path, file_path, ec);
  if (ec) {
    return Status::IOError("Failed to rename " + tmp_path + " to " + file_path + ": " +
                           ec.message());
  }
  return Status::OK();
}

/// Wait for a KV file to appear and return the string value for the given key.
/// Returns NotFound immediately if the key is missing from a valid JSON file.
inline StatusOr<std::string> WaitForKVFile(const std::string &file_path,
                                           const std::string &key,
                                           int timeout_ms = 30000,
                                           int poll_interval_ms = 100) {
  auto start = std::chrono::steady_clock::now();
  while (true) {
    if (std::filesystem::exists(file_path)) {
      std::ifstream file(file_path);
      if (file.is_open()) {
        try {
          nlohmann::json j;
          file >> j;
          file.close();
          if (j.contains(key)) {
            return j[key].get<std::string>();
          }
          return Status::NotFound("Key '" + key + "' not found in " + file_path);
        } catch (const std::exception &e) {
          return Status::IOError("Failed to parse JSON from " + file_path + ": " +
                                 e.what());
        }
      }
    }
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                       std::chrono::steady_clock::now() - start)
                       .count();
    if (elapsed >= timeout_ms) {
      return Status::TimedOut("Timed out waiting for file " + file_path);
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(poll_interval_ms));
  }
}

inline std::string GetPortFileName(const NodeID &node_id, const std::string &port_name) {
  return port_name + "_" + node_id.Hex() + ".json";
}

/// Persist a port to a file. The port is stored with key "port".
inline Status PersistPort(const std::string &dir,
                          const NodeID &node_id,
                          const std::string &port_name,
                          int port) {
  std::string file_name = GetPortFileName(node_id, port_name);
  std::string file_path = (std::filesystem::path(dir) / file_name).string();
  return WriteKVFile(file_path, "port", std::to_string(port));
}

/// Wait for a persisted port file and return the port number.
inline StatusOr<int> WaitForPersistedPort(const std::string &dir,
                                          const NodeID &node_id,
                                          const std::string &port_name,
                                          int timeout_ms = 30000,
                                          int poll_interval_ms = 100) {
  std::string file_name = GetPortFileName(node_id, port_name);
  std::string file_path = (std::filesystem::path(dir) / file_name).string();
  auto result = WaitForKVFile(file_path, "port", timeout_ms, poll_interval_ms);
  if (!result.ok()) {
    return result.status();
  }

  return std::stoi(*result);
}

}  // namespace ray
