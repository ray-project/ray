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

#include <chrono>

#include "absl/time/clock.h"
#include "absl/time/time.h"

namespace ray {

using SteadyTimePoint = std::chrono::steady_clock::time_point;

/// Interface for a clock that returns the current time.
class ClockInterface {
 public:
  virtual ~ClockInterface() = default;

  /// Wall clock time (may jump due to NTP). Use for timestamps and deadlines.
  virtual absl::Time Now() const = 0;

  /// Monotonic time (never goes backwards). Use for duration measurements.
  virtual SteadyTimePoint SteadyNow() const = 0;

  int64_t NowUnixMillis() const { return absl::ToUnixMillis(Now()); }
  int64_t NowUnixNanos() const { return absl::ToUnixNanos(Now()); }
};

/// Real clock that delegates to absl::Now() and steady_clock::now().
class Clock final : public ClockInterface {
 public:
  absl::Time Now() const override { return absl::Now(); }
  SteadyTimePoint SteadyNow() const override { return std::chrono::steady_clock::now(); }
};

/// Fake clock for deterministic testing. Time only advances when you call
/// AdvanceTime(). SteadyNow() is derived from Now() so they always agree.
class FakeClock final : public ClockInterface {
 public:
  explicit FakeClock(absl::Time start = absl::FromUnixSeconds(1000)) : now_(start) {}

  absl::Time Now() const override { return now_; }
  SteadyTimePoint SteadyNow() const override {
    return SteadyTimePoint(std::chrono::nanoseconds(absl::ToUnixNanos(now_)));
  }

  void AdvanceTime(absl::Duration duration) { now_ += duration; }

  void SetTime(absl::Time time) { now_ = time; }

 private:
  absl::Time now_;
};

}  // namespace ray
