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

#include <algorithm>
#include <array>
#include <random>
#include <string>
#include <string_view>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/synchronization/mutex.h"
#include "nlohmann/json.hpp"
#include "ray/common/ray_config.h"

namespace ray {
namespace rpc {
namespace testing {

// RpcFailureManager is a simple chaos testing framework. Before starting ray, users
// should set up os environment to use this feature for testing purposes.

// You can use this to set probabilities for specific rpc's.
//     export
//     RAY_testing_rpc_failure='{"method1":{"num_failures":3,"req_failure_prob":12,"resp_failure_prob":12,"in_flight_failure_prob":50}}'
//
// You can also use a wildcard to set probabilities for all rpc's and -1 as num_failures
// to have unlimited failures.
//     export
//     RAY_testing_rpc_failure='{"*":{"num_failures":-1,"req_failure_prob":10,"resp_failure_prob":25,"in_flight_failure_prob":50}}'
// This will set the probabilities for all rpc's to 10% for request failures, 25% for
// reply failures, and 50% for in-flight failures.

// You can also provide optional parameters to specify that there
// should be at least a certain amount of request, response, and in-flight failures.
// By default these are set to 0, but by setting them to positive values guarantees that
// the first N RPCs will have X request failures, followed by Y response failures,
// followed by Z in-flight failures. Afterwards, it will revert to the probabilistic
// failures.
//
// You can combine this with the wildcard so that each RPC method will have the same lower
// bounds applied.
//
// Ex. unlimited failures for all rpc's with 25% request failures, 50% response failures,
// and 10% in-flight failures with at least 2 request failures, 3 response failures, and 1
// in-flight failure.
//     export
//     RAY_testing_rpc_failure='{"*":{"num_failures":-1,"req_failure_prob":25,"resp_failure_prob":50,"in_flight_failure_prob":10,"num_lower_bound_req_failures":2,"num_lower_bound_resp_failures":3,"num_lower_bound_in_flight_failures":1}}'

namespace {
constexpr std::array<std::string_view, 7> kValidKeys = {
    "num_failures",
    "req_failure_prob",
    "resp_failure_prob",
    "in_flight_failure_prob",
    "num_lower_bound_req_failures",
    "num_lower_bound_resp_failures",
    "num_lower_bound_in_flight_failures"};
}  // namespace

class RpcFailureManager {
 public:
  RpcFailureManager() { Init(); }

  void Init() {
    absl::MutexLock lock(&mu_);

    // Clear old state
    failable_methods_.clear();
    num_req_failures_.clear();
    num_resp_failures_.clear();
    num_in_flight_failures_.clear();
    wildcard_set_ = false;
    has_failures_ = false;

    if (!RayConfig::instance().testing_rpc_failure().empty()) {
      auto json = nlohmann::json::parse(
          RayConfig::instance().testing_rpc_failure(), nullptr, false);
      if (json.is_discarded()) {
        RAY_LOG(FATAL) << "testing_rpc_failure is not a valid json object: "
                       << RayConfig::instance().testing_rpc_failure();
        return;
      }

      for (const auto &[method, config] : json.items()) {
        if (!config.is_object()) {
          RAY_LOG(FATAL) << "Value for method '" << method
                         << "' in testing_rpc_failure config is not a JSON object: "
                         << config.dump();
          continue;
        }
        for (const auto &[config_key, _] : config.items()) {
          if (std::find(kValidKeys.begin(), kValidKeys.end(), config_key) ==
              kValidKeys.end()) {
            RAY_LOG(FATAL) << "Unknown key specified in testing_rpc_failure config: "
                           << config_key;
          }
        }
        auto [iter, _] = failable_methods_.emplace(
            method,
            Failable{
                // The value method either retrieves the value associated with the key or
                // uses the specified default value instead.
                config.value("num_failures", 0L),
                config.value("req_failure_prob", 0UL),
                config.value("resp_failure_prob", 0UL),
                config.value("in_flight_failure_prob", 0UL),
                config.value("num_lower_bound_req_failures", 0UL),
                config.value("num_lower_bound_resp_failures", 0UL),
                config.value("num_lower_bound_in_flight_failures", 0UL),
            });
        const auto &failable = iter->second;
        RAY_CHECK_LE(failable.req_failure_prob + failable.resp_failure_prob +
                         failable.in_flight_failure_prob,
                     100UL);
        if (method == "*") {
          wildcard_set_ = true;
          // The wildcard overrides all other method configurations.
          break;
        }
      }

      std::random_device rd;
      auto seed = rd();
      RAY_LOG(INFO) << "Setting RpcFailureManager seed to " << seed;
      gen_.seed(seed);
      has_failures_ = true;
    }
  }

  RpcFailure GetRpcFailure(const std::string &name) {
    if (!has_failures_) {
      return RpcFailure::None;
    }

    absl::MutexLock lock(&mu_);

    // Wildcard overrides any other method configurations.
    if (wildcard_set_) {
      return GetFailureTypeFromFailable(failable_methods_["*"], name);
    }

    auto iter = failable_methods_.find(name);
    if (iter == failable_methods_.end()) {
      return RpcFailure::None;
    }
    return GetFailureTypeFromFailable(iter->second, name);
  }

 private:
  absl::Mutex mu_;
  std::mt19937 gen_;
  std::atomic_bool has_failures_ = false;

  // If we're testing all rpc failures, we'll use these probabilites instead of
  // failable_methods_
  bool wildcard_set_ = false;

  struct Failable {
    int64_t num_remaining_failures;
    size_t req_failure_prob;
    size_t resp_failure_prob;
    size_t in_flight_failure_prob;
    size_t num_lower_bound_req_failures = 0;
    size_t num_lower_bound_resp_failures = 0;
    size_t num_lower_bound_in_flight_failures = 0;
  };
  absl::flat_hash_map<std::string, Failable> failable_methods_ ABSL_GUARDED_BY(&mu_);

  absl::flat_hash_map<std::string, size_t> num_req_failures_ ABSL_GUARDED_BY(&mu_);
  absl::flat_hash_map<std::string, size_t> num_resp_failures_ ABSL_GUARDED_BY(&mu_);
  absl::flat_hash_map<std::string, size_t> num_in_flight_failures_ ABSL_GUARDED_BY(&mu_);

  RpcFailure GetFailureTypeFromFailable(Failable &failable, const std::string &name)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_) {
    if (failable.num_remaining_failures == 0) {
      // If < 0, unlimited failures.
      return RpcFailure::None;
    }

    if (num_req_failures_[name] < failable.num_lower_bound_req_failures) {
      failable.num_remaining_failures--;
      num_req_failures_[name]++;
      return RpcFailure::Request;
    }
    if (num_resp_failures_[name] < failable.num_lower_bound_resp_failures) {
      failable.num_remaining_failures--;
      num_resp_failures_[name]++;
      return RpcFailure::Response;
    }
    if (num_in_flight_failures_[name] < failable.num_lower_bound_in_flight_failures) {
      failable.num_remaining_failures--;
      num_in_flight_failures_[name]++;
      return RpcFailure::InFlight;
    }

    std::uniform_int_distribution<size_t> dist(1ul, 100ul);
    const size_t random_number = dist(gen_);
    if (random_number <= failable.req_failure_prob) {
      failable.num_remaining_failures--;
      return RpcFailure::Request;
    } else if (random_number <= failable.req_failure_prob + failable.resp_failure_prob) {
      failable.num_remaining_failures--;
      return RpcFailure::Response;
    } else if (random_number <= failable.req_failure_prob + failable.resp_failure_prob +
                                    failable.in_flight_failure_prob) {
      failable.num_remaining_failures--;
      return RpcFailure::InFlight;
    } else {
      return RpcFailure::None;
    }
  }
};

namespace {

RpcFailureManager &GetRpcFailureManager() {
  static auto *manager = new RpcFailureManager();
  return *manager;
}

}  // namespace

RpcFailure GetRpcFailure(const std::string &name) {
  return GetRpcFailureManager().GetRpcFailure(name);
}

void Init() { GetRpcFailureManager().Init(); }

}  // namespace testing
}  // namespace rpc
}  // namespace ray
