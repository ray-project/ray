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
#include <cstdint>
#include <functional>
#include <utility>
#include <vector>

#include "absl/synchronization/mutex.h"
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

  /// Convenience: current time as Unix milliseconds.
  int64_t NowUnixMillis() const { return absl::ToUnixMillis(Now()); }

  /// Convenience: current time as Unix microseconds.
  int64_t NowUnixMicros() const { return absl::ToUnixMicros(Now()); }

  /// Convenience: current time as Unix nanoseconds.
  int64_t NowUnixNanos() const { return absl::ToUnixNanos(Now()); }
};

/// Real clock that delegates to absl::Now() and steady_clock::now(). Thread-safe.
class Clock final : public ClockInterface {
 public:
  absl::Time Now() const override { return absl::Now(); }
  SteadyTimePoint SteadyNow() const override { return std::chrono::steady_clock::now(); }
};

/// Fake clock for deterministic testing. Time only advances when you call
/// AdvanceTime(). SteadyNow() is derived from Now() so they always agree.
/// Thread-safe.
class FakeClock final : public ClockInterface {
 public:
  explicit FakeClock(absl::Time start = absl::FromUnixSeconds(1000));

  absl::Time Now() const override;

  SteadyTimePoint SteadyNow() const override;

  void AdvanceTime(absl::Duration duration);

  void SetTime(absl::Time time);

  /// Register a callback that is invoked whenever the clock's time changes (via
  /// AdvanceTime or SetTime). The callback is passed the new current time.
  /// Returns a handle that can be passed to UnregisterOnAdvanceCallback to
  /// remove the callback.
  ///
  /// This is intentionally generic: the clock has no knowledge of who registers
  /// callbacks, which keeps it loosely coupled from its observers (e.g. a fake
  /// periodical runner).
  uint64_t RegisterOnAdvanceCallback(std::function<void(absl::Time)> callback);

  /// Remove a callback previously registered with RegisterOnAdvanceCallback.
  /// No-op if the handle is not registered.
  void UnregisterOnAdvanceCallback(uint64_t handle);

 private:
  // Invoke all registered callbacks with the current time. Callbacks are invoked
  // without the lock held, so they may safely call back into the clock (e.g.
  // Now()) without deadlocking.
  //
  // We snapshot the callback *handles* up front, then re-look-up each handle
  // under the lock immediately before invoking it. This guards against a
  // use-after-free: one callback may destroy another observer, whose destructor
  // calls UnregisterOnAdvanceCallback() to remove its (now-dangling) callback.
  // Re-checking the handle ensures we never invoke a callback that was
  // unregistered during this notification. Newly registered callbacks are not
  // picked up until the next notification, which matches the snapshot semantics.
  void NotifyTimeChanged();

  mutable absl::Mutex mu_;
  absl::Time now_ ABSL_GUARDED_BY(mu_);
  uint64_t next_callback_handle_ ABSL_GUARDED_BY(mu_) = 0;
  std::vector<std::pair<uint64_t, std::function<void(absl::Time)>>> callbacks_
      ABSL_GUARDED_BY(mu_);
};

}  // namespace ray
