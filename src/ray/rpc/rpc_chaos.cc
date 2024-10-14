// Copyright 2024 The Ray Authors.
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

#include "ray/rpc/rpc_chaos.h"

#include <unordered_set>

#include "ray/common/ray_config.h"

namespace ray {
namespace rpc {
namespace testing {
namespace {

class RpcFailureManager {
 public:
  RpcFailureManager() { Init(); }

  void Init() {
    failable_methods_.clear();

    if (!RayConfig::instance().testing_rpc_failure().empty()) {
      for (const auto &method :
           absl::StrSplit(RayConfig::instance().testing_rpc_failure(), ",")) {
        failable_methods_.emplace(method);
      }
    }
  }

  RpcFailure GetRpcFailure(const std::string &name) {
    if (failable_methods_.find(name) == failable_methods_.end()) {
      return RpcFailure::None;
    }

    int rand = std::rand() % 4;
    if (rand == 0) {
      // 25% chance
      return RpcFailure::Request;
    } else if (rand == 1) {
      // 25% chance
      return RpcFailure::Response;
    } else {
      // 50% chance
      return RpcFailure::None;
    }
  }

 private:
  std::unordered_set<std::string> failable_methods_;
};

static RpcFailureManager _rpc_failure_manager;

}  // namespace

RpcFailure get_rpc_failure(const std::string &name) {
  if (RayConfig::instance().testing_rpc_failure().empty()) {
    return RpcFailure::None;
  }
  return _rpc_failure_manager.GetRpcFailure(name);
}

void init() { _rpc_failure_manager.Init(); }

}  // namespace testing
}  // namespace rpc
}  // namespace ray
