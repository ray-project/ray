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

#include "absl/synchronization/mutex.h"
#include "absl/time/clock.h"
#include "absl/time/time.h"

namespace ray {

/// Interface for a clock that returns the current time.
class ClockInterface {
 public:
  virtual ~ClockInterface() = default;
  virtual absl::Time Now() const = 0;

  /// Convenience: current time as Unix microseconds.
  int64_t NowUnixMicros() const { return absl::ToUnixMicros(Now()); }

  /// Convenience: current time as Unix milliseconds.
  int64_t NowUnixMillis() const { return absl::ToUnixMillis(Now()); }

  /// Convenience: current time as Unix nanoseconds.
  int64_t NowUnixNanos() const { return absl::ToUnixNanos(Now()); }
};

/// Real clock that delegates to absl::Now().
class Clock final : public ClockInterface {
 public:
  absl::Time Now() const override { return absl::Now(); }
};

/// Fake clock for deterministic testing. Time only advances when you call
/// AdvanceTime(). Thread-safe.
class FakeClock final : public ClockInterface {
 public:
  explicit FakeClock(absl::Time start = absl::FromUnixSeconds(1000)) : now_(start) {}

  absl::Time Now() const override {
    absl::MutexLock lock(&mu_);
    return now_;
  }

  void AdvanceTime(absl::Duration duration) {
    absl::MutexLock lock(&mu_);
    now_ += duration;
  }

  void SetTime(absl::Time time) {
    absl::MutexLock lock(&mu_);
    now_ = time;
  }

 private:
  mutable absl::Mutex mu_;
  absl::Time now_ ABSL_GUARDED_BY(mu_);
};

}  // namespace ray
