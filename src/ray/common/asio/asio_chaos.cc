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

#include <string_view>
#include <cstdlib>

#include "absl/container/flat_hash_map.h"
#include "absl/strings/numbers.h"
#include "absl/strings/str_split.h"
#include "ray/util/logging.h"

namespace ray {
namespace asio {
namespace testing {

namespace {
class DelayManager {
 public:
  DelayManager() {
    auto delay_env = std::getenv("RAY_TESTING_ASIO_DELAY");
    if(delay_env == nullptr) {
      return;
    }
    std::vector<std::string_view> items = absl::StrSplit(delay_env, ",");
    for(const auto& item : items) {
      std::vector<std::string_view> delay = absl::StrSplit(item, "=");
      size_t delay_ms = 0;
      if(delay.size() != 2 || !absl::SimpleAtoi(delay[1], &delay_ms)) {
        RAY_LOG(ERROR) << "Error syntax: " << item << ", it has to be name=time";
        continue;
      }
      RAY_LOG(WARNING) << "Inject asio delay on method = " << delay[0] << " for " << delay_ms << "ms";
      if(delay[0] == "*") {
        random_delay_ms_ = delay_ms;
      } else {
        delay_[std::string{delay[0]}] = delay_ms;
      }
    }
  }

  size_t GetMethodDelay(const std::string& name) {
    auto it = delay_.find(name);
    if(it == delay_.end()) {
      return GenRandomDelay(random_delay_ms_);
    }
    return GenRandomDelay(it->second);
  }

 private:
  size_t GenRandomDelay(size_t delay_ms) const {
    if(delay_ms == 0) {
      return 0;
    }
    return std::rand() % delay_ms;
  }

  absl::flat_hash_map<std::string, size_t> delay_;
  size_t random_delay_ms_ = 0;
};

static DelayManager _delay_manager;
}

size_t get_delay_ms(const std::string& name) {
  return _delay_manager.GetMethodDelay(name);
}

}
}
}
