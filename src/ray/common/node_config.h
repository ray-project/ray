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

#include <string>

namespace ray {

// Global node configuration that can be accessed from anywhere in the Ray core.
class NodeConfig {
 public:
  static NodeConfig &Instance() {
    static NodeConfig instance;
    return instance;
  }

  void SetNodeIpAddress(const std::string &ip) { node_ip_address_ = ip; }

  const std::string &GetNodeIpAddress() const { return node_ip_address_; }

  bool HasNodeIpAddress() const { return !node_ip_address_.empty(); }

 private:
  NodeConfig() = default;
  NodeConfig(const NodeConfig &) = delete;
  NodeConfig &operator=(const NodeConfig &) = delete;

  std::string node_ip_address_;
};

}  // namespace ray
