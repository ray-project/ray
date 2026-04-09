// Copyright 2026 The Ray Authors.
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

#include "absl/time/clock.h"
#include "absl/time/time.h"

namespace ray {

/// Interface for a clock that returns the current time.
class ClockInterface {
 public:
  virtual ~ClockInterface() = default;
  virtual absl::Time Now() = 0;
};

/// Real clock that delegates to absl::Now().
class Clock : public ClockInterface {
 public:
  absl::Time Now() override { return absl::Now(); }
};

/// Fake clock for deterministic testing. Time only advances when you call
/// AdvanceTime().
class FakeClock : public ClockInterface {
 public:
  explicit FakeClock(absl::Time start = absl::FromUnixSeconds(1000)) : now_(start) {}

  absl::Time Now() override { return now_; }

  void AdvanceTime(absl::Duration duration) { now_ += duration; }

  void SetTime(absl::Time time) { now_ = time; }

 private:
  absl::Time now_;
};

}  // namespace ray
