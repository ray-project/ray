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

#include "ray/core_worker/generator_waiter.h"

namespace ray {
namespace core {

GeneratorBackpressureWaiter::GeneratorBackpressureWaiter(
    int64_t generator_backpressure_num_objects)
    : backpressure_threshold_(generator_backpressure_num_objects),
      total_objects_generated_(0),
      total_objects_consumed_(0) {
  // 0 makes no sense, and it is not supported.
  RAY_CHECK_NE(generator_backpressure_num_objects, 0);
}

Status GeneratorBackpressureWaiter::WaitUntilObjectConsumed(
    std::function<Status()> check_signals) {
  if (backpressure_threshold_ < 0) {
    RAY_CHECK_EQ(backpressure_threshold_, -1);
    // Backpressure disabled if backpressure_threshold_ == -1.
    return Status::OK();
  }
  RAY_CHECK(check_signals != nullptr);

  absl::MutexLock lock(&mutex_);

  auto return_status = Status::OK();
  auto total_object_unconsumed = total_objects_generated_ - total_objects_consumed_;
  if (total_object_unconsumed >= backpressure_threshold_) {
    RAY_LOG(DEBUG) << "Generator backpressured, consumed: " << total_objects_consumed_
                   << ". generated: " << total_objects_generated_
                   << ". threshold: " << backpressure_threshold_;
    while (total_object_unconsumed >= backpressure_threshold_) {
      cond_var_.WaitWithTimeout(&mutex_, absl::Seconds(1));
      total_object_unconsumed = total_objects_generated_ - total_objects_consumed_;
      return_status = check_signals();
      if (!return_status.ok()) {
        break;
      }
    }
  }
  return return_status;
}

void GeneratorBackpressureWaiter::UpdateTotalObjectConsumed(
    int64_t total_objects_consumed) {
  absl::MutexLock lock(&mutex_);
  total_objects_consumed_ = std::max(total_objects_consumed, total_objects_consumed_);
  auto total_object_unconsumed = total_objects_generated_ - total_objects_consumed_;
  if (total_object_unconsumed < backpressure_threshold_) {
    cond_var_.SignalAll();
  }
}

void GeneratorBackpressureWaiter::IncrementObjectGenerated() {
  absl::MutexLock lock(&mutex_);
  total_objects_generated_ += 1;
}

int64_t GeneratorBackpressureWaiter::TotalObjectConsumed() {
  absl::MutexLock lock(&mutex_);
  return total_objects_consumed_;
}

int64_t GeneratorBackpressureWaiter::TotalObjectGenerated() {
  absl::MutexLock lock(&mutex_);
  return total_objects_generated_;
}

}  // namespace core
}  // namespace ray
