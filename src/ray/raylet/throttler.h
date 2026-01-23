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

#pragma once

#include <functional>
#include <utility>

#include "absl/time/clock.h"

namespace ray {

class Throttler {
 public:
  explicit Throttler(int64_t interval_ns, std::function<int64_t()> now = nullptr)
      : now_(now == nullptr ? []() { return absl::GetCurrentTimeNanos(); }
                            : std::move(now)),
        interval_ns_(interval_ns),
        // Subtracting interval so the first run is possible.
        last_run_ns_(now_() - interval_ns) {}

  bool CheckAndUpdateIfPossible() {
    auto now = now_();
    if (now - last_run_ns_ >= interval_ns_) {
      last_run_ns_ = now;
      return true;
    }
    return false;
  }

  uint64_t LastRunTime() const { return last_run_ns_; }

 private:
  std::function<int64_t()> now_;
  uint64_t interval_ns_;
  uint64_t last_run_ns_;
};

}  // namespace ray
