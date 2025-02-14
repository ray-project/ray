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

namespace ray {
namespace asio {
namespace testing {

namespace {
/*
  DelayManager is a simple chaos testing framework. Before starting ray, users
  should set up os environment to use this feature for testing purposes.

  To use this, simply do
      export RAY_testing_asio_delay_us="method1=10:10,method2=20:100"

   The delay is a random number between the left and right of colon. If method equals '*',
   it will apply to all methods.

   Please check warnings to make sure delay is on.
*/
class DelayManager {
 public:
  DelayManager() { Init(); }

  int64_t GetMethodDelay(const std::string &name) const {
    auto it = delay_.find(name);
    if (it == delay_.end()) {
      return GenRandomDelay(default_delay_range_us_.first,
                            default_delay_range_us_.second);
    }
    int64_t actual_delay = GenRandomDelay(it->second.first, it->second.second);
    if (actual_delay != 0) {
      RAY_LOG_EVERY_N(ERROR, 1000)
          << "Delaying method " << name << " for " << actual_delay << "us";
    }
    return actual_delay;
  }

  void Init() {
    delay_.clear();
    default_delay_range_us_ = {0, 0};
    auto delay_env = RayConfig::instance().testing_asio_delay_us();
    if (delay_env.empty()) {
      return;
    }
    std::cerr << "RAY_testing_asio_delay_us is set to " << delay_env << std::endl;
    std::vector<std::string_view> items = absl::StrSplit(delay_env, ",");
    for (const auto &item : items) {
      ParseItem(item);
    }
  }

 private:
  // method1=min:max
  void ParseItem(std::string_view val) {
    std::vector<std::string_view> item_val = absl::StrSplit(val, "=");
    if (item_val.size() != 2) {
      std::cerr << "Error in syntax: " << val
                << ", expected method=min_us:max:ms. Skip this entry." << std::endl;
      _Exit(1);
    }
    auto delay_us = ParseVal(item_val[1]);
    if (item_val[0] == "*") {
      default_delay_range_us_ = delay_us;
    } else {
      delay_[item_val[0]] = delay_us;
    }
  }

  std::pair<int64_t, int64_t> ParseVal(std::string_view val) {
    std::vector<std::string_view> delay_str_us = absl::StrSplit(val, ":");
    if (delay_str_us.size() != 2) {
      std::cerr << "Error in syntax: " << val
                << ", expected method=min_us:max:ms. Skip this entry" << std::endl;
      _Exit(1);
    }
    std::pair<int64_t, int64_t> delay_us;
    if (!absl::SimpleAtoi(delay_str_us[0], &delay_us.first) ||
        !absl::SimpleAtoi(delay_str_us[1], &delay_us.second)) {
      std::cerr << "Error in syntax: " << val
                << ", expected method=min_us:max:ms. Skip this entry" << std::endl;
      _Exit(1);
    }
    if (delay_us.first > delay_us.second) {
      std::cerr << delay_us.first << " is bigger than " << delay_us.second
                << ". Skip this entry." << std::endl;
      _Exit(1);
    }
    return delay_us;
  }

  int64_t GenRandomDelay(int64_t min_delay_us, int64_t max_delay_us) const {
    if (min_delay_us == max_delay_us) {
      return min_delay_us;
    }
    return std::rand() % (max_delay_us - min_delay_us) + min_delay_us;
  }

  absl::flat_hash_map<std::string, std::pair<int64_t, int64_t>> delay_;
  std::pair<int64_t, int64_t> default_delay_range_us_ = {0, 0};
};

static DelayManager _delay_manager;
}  // namespace

int64_t get_delay_us(const std::string &name) {
  if (RayConfig::instance().testing_asio_delay_us().empty()) {
    return 0;
  }
  return _delay_manager.GetMethodDelay(name);
}

void init() { return _delay_manager.Init(); }  // namespace testing

}  // namespace testing
}  // namespace asio
}  // namespace ray
