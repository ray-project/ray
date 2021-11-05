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

#include "ray/common/asio/asio_chaos.h"

#include <cstdlib>
#include <string_view>

#include "absl/container/flat_hash_map.h"
#include "absl/strings/numbers.h"
#include "absl/strings/str_split.h"
#include "ray/common/ray_config.h"
#include "ray/util/logging.h"

namespace ray {
namespace asio {
namespace testing {

namespace {
/*
  DelayManager is a simple chaos testing framework. Before starting ray, users
  should set up os environment to use this feature for testing purposes.

  To use this, simply do
      export RAY_testing_asio_delay_ms="method1=10,method2=20"

   The delay is a random number between 0 and the value. If method equals '*',
   it will apply to all methods.

   Please check warnings to make sure delay is on.
*/
class DelayManager {
 public:
  DelayManager() {
    // RAY_testing_asio_delay="Method1=100,Method2=200"
    auto delay_env = RayConfig::instance().testing_asio_delay_ms();
    if (delay_env.empty()) {
      return;
    }
    RAY_LOG(ERROR) << "RAY_testing_asio_delay_ms is set to " << delay_env;
    std::vector<std::string_view> items = absl::StrSplit(delay_env, ",");
    for (const auto &item : items) {
      std::vector<std::string_view> delay = absl::StrSplit(item, "=");
      int64_t delay_ms = 0;
      if (delay.size() != 2 || !absl::SimpleAtoi(delay[1], &delay_ms)) {
        RAY_LOG(ERROR) << "Error in syntax: " << item << ", expected name=time";
        continue;
      }
      RAY_LOG(WARNING) << "Inject asio delay on method = " << delay[0] << " for "
                       << delay_ms << "ms";
      if (delay[0] == "*") {
        random_delay_ms_ = delay_ms;
      } else {
        delay_[std::string{delay[0]}] = delay_ms;
      }
    }
  }

  int64_t GetMethodDelay(const std::string &name) {
    auto it = delay_.find(name);
    if (it == delay_.end()) {
      return GenRandomDelay(random_delay_ms_);
    }
    auto actual_delay = GenRandomDelay(it->second);
    if (actual_delay != 0) {
      RAY_LOG_EVERY_N(DEBUG, 1000)
          << "Delaying method " << name << " for " << actual_delay << "ms";
    }
    return actual_delay;
  }

 private:
  int64_t GenRandomDelay(size_t delay_ms) const {
    if (delay_ms == 0) {
      return 0;
    }
    return std::rand() % delay_ms;
  }

  absl::flat_hash_map<std::string, int64_t> delay_;
  int64_t random_delay_ms_ = 0;
};

static DelayManager _delay_manager;
}  // namespace

int64_t get_delay_ms(const std::string &name) {
  return _delay_manager.GetMethodDelay(name);
}

}  // namespace testing
}  // namespace asio
}  // namespace ray
