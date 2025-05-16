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
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/synchronization/mutex.h"
#include "ray/common/ray_config.h"

namespace ray {
namespace rpc {
namespace testing {
namespace {

// RpcFailureManager is a simple chaos testing framework. Before starting ray, users
// should set up os environment to use this feature for testing purposes.
// To use this, simply do
//     export RAY_testing_rpc_failure="method1=3:25:50,method2=5:25:25"
// Key is the RPC call name and value is a three part colon separated structure. It
// contains the max number of failures to inject + probability of req failure +
// probability of reply failure.

class RpcFailureManager {
 public:
  RpcFailureManager() { Init(); }

  void Init() {
    absl::MutexLock lock(&mu_);

    failable_methods_.clear();

    if (!RayConfig::instance().testing_rpc_failure().empty()) {
      for (const auto &item :
           absl::StrSplit(RayConfig::instance().testing_rpc_failure(), ',')) {
        std::vector<std::string> equal_split = absl::StrSplit(item, '=');
        RAY_CHECK_EQ(equal_split.size(), 2UL);
        std::vector<std::string> colon_split = absl::StrSplit(equal_split[1], ':');
        RAY_CHECK_EQ(colon_split.size(), 3UL);
        auto [iter, _] = failable_methods_.emplace(equal_split[0],
                                                   Failable{std::stoul(colon_split[0]),
                                                            std::stoul(colon_split[1]),
                                                            std::stoul(colon_split[2])});
        const auto &failable = iter->second;
        RAY_CHECK_LE(failable.req_failure_prob + failable.resp_failure_prob, 100UL);
      }

      std::random_device rd;
      auto seed = rd();
      RAY_LOG(INFO) << "Setting RpcFailureManager seed to " << seed;
      gen_.seed(seed);
    }
  }

  RpcFailure GetRpcFailure(const std::string &name) {
    absl::MutexLock lock(&mu_);

    auto iter = failable_methods_.find(name);
    if (iter == failable_methods_.end()) {
      return RpcFailure::None;
    }

    auto &failable = iter->second;
    if (failable.num_remaining_failures == 0) {
      return RpcFailure::None;
    }

    std::uniform_int_distribution<size_t> dist(1ul, 100ul);
    const size_t random_number = dist(gen_);
    if (random_number <= failable.req_failure_prob) {
      failable.num_remaining_failures--;
      return RpcFailure::Request;
    }
    if (random_number <= failable.req_failure_prob + failable.resp_failure_prob) {
      failable.num_remaining_failures--;
      return RpcFailure::Response;
    }
    return RpcFailure::None;
  }

 private:
  absl::Mutex mu_;
  std::mt19937 gen_;
  struct Failable {
    size_t num_remaining_failures;
    size_t req_failure_prob;
    size_t resp_failure_prob;
  };
  // call name -> (num_remaining_failures, req_failure_prob, resp_failure_prob)
  absl::flat_hash_map<std::string, Failable> failable_methods_ ABSL_GUARDED_BY(&mu_);
};

auto &rpc_failure_manager = []() -> RpcFailureManager & {
  static auto *manager = new RpcFailureManager();
  return *manager;
}();

}  // namespace

RpcFailure GetRpcFailure(const std::string &name) {
  if (RayConfig::instance().testing_rpc_failure().empty()) {
    return RpcFailure::None;
  }
  return rpc_failure_manager.GetRpcFailure(name);
}

void Init() { rpc_failure_manager.Init(); }

}  // namespace testing
}  // namespace rpc
}  // namespace ray
