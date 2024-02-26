// Copyright 2023 The Ray Authors.
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
#include "ray/core_worker/common.h"

namespace ray {
namespace core {

/// A class to pause a task execution while a generator is backpressured.
class GeneratorBackpressureWaiter {
 public:
  GeneratorBackpressureWaiter(int64_t generator_backpressure_num_objects);
  /// Block a thread and wait until objects are consumed from a consumer.
  /// It return OK status if the backpressure is not needed or backpressure is
  /// finished.
  /// It periodically checks the signals (Python code has to keep checking
  /// signals while it is blocked by cpp) using a callback `check_signals`.
  /// If check_signals returns non-ok status, it finishes blocking and returns
  /// the non-ok status to the caller.
  Status WaitUntilObjectConsumed(std::function<Status()> check_signals);
  void UpdateTotalObjectConsumed(int64_t total_objects_consumed);
  void IncrementObjectGenerated();
  int64_t TotalObjectConsumed();
  int64_t TotalObjectGenerated();

 private:
  absl::Mutex mutex_;
  absl::CondVar cond_var_;
  // If total_objects_generated_ - total_objects_consumed_ < this
  // the task will stop.
  const int64_t backpressure_threshold_;
  // Total number of objects generated from a generator.
  int64_t total_objects_generated_;
  // Total number of objects consumed from a generator.
  int64_t total_objects_consumed_;
};

}  // namespace core
}  // namespace ray