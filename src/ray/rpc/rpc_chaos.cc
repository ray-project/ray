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

#include <random>
#include <string>
#include <unordered_map>
#include <vector>

#include "absl/synchronization/mutex.h"
#include "ray/common/ray_config.h"

namespace ray {
namespace rpc {
namespace testing {
namespace {

/*
  RpcFailureManager is a simple chaos testing framework. Before starting ray, users
  should set up os environment to use this feature for testing purposes.
  To use this, simply do
      export RAY_testing_rpc_failure="method1=3,method2=5"
   Key is the RPC call name and value is the max number of failures to inject.
*/
class RpcFailureManager {
 public:
  RpcFailureManager() { Init(); }

  void Init() {
    absl::MutexLock lock(&mu_);

    failable_methods_.clear();

    if (!RayConfig::instance().testing_rpc_failure().empty()) {
      for (const auto &item :
           absl::StrSplit(RayConfig::instance().testing_rpc_failure(), ",")) {
        std::vector<std::string> parts = absl::StrSplit(item, "=");
        RAY_CHECK_EQ(parts.size(), 2UL);
        failable_methods_.emplace(parts[0], std::atoi(parts[1].c_str()));
      }

      std::random_device rd;
      auto seed = rd();
      RAY_LOG(INFO) << "Setting RpcFailureManager seed to " << seed;
      gen_.seed(seed);
    }
  }

  RpcFailure GetRpcFailure(const std::string &name) {
    absl::MutexLock lock(&mu_);

    if (failable_methods_.find(name) == failable_methods_.end()) {
      return RpcFailure::None;
    }

    uint64_t &num_remaining_failures = failable_methods_.at(name);
    if (num_remaining_failures == 0) {
      return RpcFailure::None;
    }

    std::uniform_int_distribution<int> dist(0, 3);
    int rand = dist(gen_);
    if (rand == 0) {
      // 25% chance
      num_remaining_failures--;
      return RpcFailure::Request;
    } else if (rand == 1) {
      // 25% chance
      num_remaining_failures--;
      return RpcFailure::Response;
    } else {
      // 50% chance
      return RpcFailure::None;
    }
  }

 private:
  absl::Mutex mu_;
  std::mt19937 gen_;
  // call name -> # remaining failures
  std::unordered_map<std::string, uint64_t> failable_methods_ ABSL_GUARDED_BY(&mu_);
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
