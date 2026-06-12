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

#include <cstdint>
#include <functional>
#include <string>
#include <vector>

#include "absl/base/thread_annotations.h"
#include "absl/synchronization/mutex.h"
#include "absl/time/time.h"
#include "ray/asio/periodical_runner_interface.h"
#include "ray/util/clock.h"

namespace ray {

/// \class FakePeriodicalRunner
/// A deterministic, test-only PeriodicalRunner driven by a FakeClock instead of
/// a real event loop.
///
/// Each registered function is scheduled to run every `period_ms`. The runner
/// is purely time-driven: it reads the clock's current time at construction as
/// its scheduling reference point and registers an on-advance callback with the
/// clock. Whenever the clock's time is advanced (via AdvanceTime/SetTime), the
/// clock calls back into the runner, which invokes every function whose next
/// scheduled run time has been reached. If more than one period has elapsed
/// since the last advance, the function is invoked once per elapsed period
/// ("catch up").
///
///   FakeClock clock;
///   FakePeriodicalRunner runner(clock);
///   runner.RunFnPeriodically(fn, /*period_ms=*/100, "fn");
///   clock.AdvanceTime(absl::Milliseconds(100));  // invokes fn once
///
/// The coupling to the clock is loose: the runner only uses the clock's generic
/// Now()/RegisterOnAdvanceCallback()/UnregisterOnAdvanceCallback() APIs, and the
/// clock has no knowledge of the runner. The runner unregisters its callback
/// from the clock on destruction, so it is safe to destroy the runner before
/// the clock.
class FakePeriodicalRunner : public PeriodicalRunnerInterface {
 public:
  /// Construct a runner driven by `clock`. Reads `clock.Now()` as the initial
  /// scheduling reference and registers an on-advance callback with the clock.
  /// `clock` must outlive this runner.
  explicit FakePeriodicalRunner(FakeClock &clock);

  ~FakePeriodicalRunner() override;

  FakePeriodicalRunner(const FakePeriodicalRunner &) = delete;
  FakePeriodicalRunner &operator=(const FakePeriodicalRunner &) = delete;

  void RunFnPeriodically(std::function<void()> fn,
                         uint64_t period_ms,
                         std::string name) override ABSL_LOCKS_EXCLUDED(mutex_);

 private:
  struct PeriodicTask {
    std::function<void()> fn;
    absl::Duration period;
    // The next time at which `fn` should be invoked.
    absl::Time next_run;
    std::string name;
  };

  // Invoked by the clock whenever its time advances. Runs every task whose next
  // scheduled run time is at or before `now`, once per elapsed period.
  void OnTimeAdvanced(absl::Time now) ABSL_LOCKS_EXCLUDED(mutex_);

  FakeClock &clock_;
  // Handle for the callback registered with `clock_`, used to unregister on
  // destruction.
  uint64_t callback_handle_;

  mutable absl::Mutex mutex_;
  std::vector<PeriodicTask> tasks_ ABSL_GUARDED_BY(mutex_);
  // The most recent time observed via the clock (at construction or
  // OnTimeAdvanced), used as the reference point for scheduling newly
  // registered tasks.
  absl::Time now_ ABSL_GUARDED_BY(mutex_);
};

}  // namespace ray
