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

#include <chrono>
#include <functional>
#include <utility>

#include "ray/util/clock.h"

namespace ray {

class Throttler {
 public:
  explicit Throttler(int64_t interval_ns, ClockInterface &clock)
      : clock_(clock),
        interval_(std::chrono::nanoseconds(interval_ns)),
        // Subtracting interval so the first run is possible.
        last_run_(clock_.SteadyNow() - interval_) {}

  bool CheckAndUpdateIfPossible() {
    auto now = clock_.SteadyNow();
    if (now - last_run_ >= interval_) {
      last_run_ = now;
      return true;
    }
    return false;
  }

  SteadyTimePoint LastRunTime() const { return last_run_; }

 private:
  ClockInterface &clock_;
  std::chrono::nanoseconds interval_;
  SteadyTimePoint last_run_;
};

}  // namespace ray
