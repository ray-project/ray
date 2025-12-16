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

#include "ray/util/port_persistence.h"

#include <filesystem>
#include <string>

#include "ray/util/file_persistence.h"

namespace ray {

std::string GetPortFileName(const NodeID &node_id, const std::string &port_name) {
  return port_name + "_" + node_id.Hex();
}

Status PersistPort(const std::string &dir,
                   const NodeID &node_id,
                   const std::string &port_name,
                   int port) {
  std::string file_name = GetPortFileName(node_id, port_name);
  std::string file_path = (std::filesystem::path(dir) / file_name).string();
  return WriteFile(file_path, std::to_string(port));
}

StatusOr<int> WaitForPersistedPort(const std::string &dir,
                                   const NodeID &node_id,
                                   const std::string &port_name,
                                   int timeout_ms,
                                   int poll_interval_ms) {
  std::string file_name = GetPortFileName(node_id, port_name);
  std::string file_path = (std::filesystem::path(dir) / file_name).string();
  auto result = WaitForFile(file_path, timeout_ms, poll_interval_ms);
  if (!result.ok()) {
    return result.status();
  }

  try {
    return std::stoi(*result);
  } catch (const std::exception &e) {
    return Status::Invalid("Invalid port value in file " + file_path + ": " + *result);
  }
}

}  // namespace ray
