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

#include "ray/asio/fake_periodical_runner.h"

#include <functional>
#include <string>
#include <utility>
#include <vector>

namespace ray {

FakePeriodicalRunner::FakePeriodicalRunner(FakeClock &clock) : clock_(&clock) {
  now_ = clock.Now();
  callback_handle_ = clock.RegisterOnAdvanceCallback(
      [this](absl::Time now) { OnTimeAdvanced(now); });
}

FakePeriodicalRunner::~FakePeriodicalRunner() {
  if (clock_ != nullptr) {
    clock_->UnregisterOnAdvanceCallback(callback_handle_);
  }
}

void FakePeriodicalRunner::RunFnPeriodically(std::function<void()> fn,
                                             uint64_t period_ms,
                                             std::string name) {
  // Matches PeriodicalRunner: a non-positive period is ignored.
  if (period_ms == 0) {
    return;
  }
  absl::Duration period = absl::Milliseconds(period_ms);
  absl::MutexLock lock(&mutex_);
  tasks_.push_back(PeriodicTask{std::move(fn),
                                period,
                                /*next_run=*/now_ + period,
                                std::move(name)});
}

void FakePeriodicalRunner::OnTimeAdvanced(absl::Time now) {
  // Collect all due invocations under the lock, then run them without it held
  // so that the callbacks may safely re-enter the runner (e.g. by registering
  // new functions) without deadlocking.
  std::vector<std::function<void()>> to_run;
  {
    absl::MutexLock lock(&mutex_);
    now_ = now;
    for (auto &task : tasks_) {
      // Invoke once per elapsed period to catch up on large time jumps.
      while (task.next_run <= now) {
        to_run.push_back(task.fn);
        task.next_run += task.period;
      }
    }
  }
  for (const auto &fn : to_run) {
    fn();
  }
}

size_t FakePeriodicalRunner::NumRegistered() const {
  absl::MutexLock lock(&mutex_);
  return tasks_.size();
}

}  // namespace ray
