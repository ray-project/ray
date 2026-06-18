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

#include "ray/util/clock.h"

#include <chrono>
#include <cstdint>
#include <functional>
#include <utility>
#include <vector>

#include "absl/synchronization/mutex.h"
#include "absl/time/time.h"

namespace ray {

FakeClock::FakeClock(absl::Time start) : now_(start) {}

absl::Time FakeClock::Now() const {
  absl::MutexLock lock(&mu_);
  return now_;
}

SteadyTimePoint FakeClock::SteadyNow() const {
  return SteadyTimePoint(std::chrono::nanoseconds(absl::ToUnixNanos(Now())));
}

void FakeClock::AdvanceTime(absl::Duration duration) {
  {
    absl::MutexLock lock(&mu_);
    now_ += duration;
  }
  NotifyTimeChanged();
}

void FakeClock::SetTime(absl::Time time) {
  {
    absl::MutexLock lock(&mu_);
    now_ = time;
  }
  NotifyTimeChanged();
}

uint64_t FakeClock::RegisterOnAdvanceCallback(std::function<void(absl::Time)> callback) {
  absl::MutexLock lock(&mu_);
  uint64_t handle = next_callback_handle_++;
  callbacks_.emplace_back(handle, std::move(callback));
  return handle;
}

void FakeClock::UnregisterOnAdvanceCallback(uint64_t handle) {
  absl::MutexLock lock(&mu_);
  for (auto it = callbacks_.begin(); it != callbacks_.end(); ++it) {
    if (it->first == handle) {
      callbacks_.erase(it);
      return;
    }
  }
}

void FakeClock::NotifyTimeChanged() {
  absl::Time now;
  std::vector<uint64_t> handles;
  {
    absl::MutexLock lock(&mu_);
    now = now_;
    handles.reserve(callbacks_.size());
    for (const auto &entry : callbacks_) {
      handles.push_back(entry.first);
    }
  }
  for (uint64_t handle : handles) {
    std::function<void(absl::Time)> callback;
    {
      absl::MutexLock lock(&mu_);
      for (const auto &entry : callbacks_) {
        if (entry.first == handle) {
          callback = entry.second;
          break;
        }
      }
    }
    if (callback) {
      callback(now);
    }
  }
}

}  // namespace ray
