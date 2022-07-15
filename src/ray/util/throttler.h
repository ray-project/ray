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
#include <iostream>

#include "absl/time/clock.h"
namespace ray {

class Throttler {
 public:
  explicit Throttler(int64_t interval_ns, std::function<int64_t()> now = nullptr)
      : last_run_ns_(0), interval_ns_(interval_ns), now_(now) {}

  bool AbleToRun() {
    auto now = Now();
    if (now - last_run_ns_ >= interval_ns_) {
      last_run_ns_ = now;
      return true;
    }
    return false;
  }

  void RunNow() { last_run_ns_ = Now(); }

 private:
  int64_t Now() {
    if (now_) {
      return now_();
    } else {
      return absl::GetCurrentTimeNanos();
    }
  }

  uint64_t last_run_ns_;
  uint64_t interval_ns_;
  std::function<int64_t()> now_;
};

}  // namespace ray
