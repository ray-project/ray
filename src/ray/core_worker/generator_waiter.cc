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

#include <algorithm>
#include <utility>

namespace ray {
namespace core {

TaskGeneratorBackpressureWaiter::TaskGeneratorBackpressureWaiter(
    int64_t generator_backpressure_num_objects, std::function<Status()> check_signals)
    : backpressure_threshold_(generator_backpressure_num_objects),
      check_signals_(std::move(check_signals)) {
  RAY_CHECK_NE(generator_backpressure_num_objects, 0);
  RAY_CHECK(check_signals_ != nullptr);
}

Status TaskGeneratorBackpressureWaiter::WaitUntilObjectConsumed() {
  if (backpressure_threshold_ < 0) {
    RAY_CHECK_EQ(backpressure_threshold_, -1);
    return Status::OK();
  }

  absl::MutexLock lock(&mutex_);

  auto return_status = Status::OK();
  auto total_object_unconsumed = total_objects_generated_ - total_objects_consumed_;
  if (total_object_unconsumed >= backpressure_threshold_) {
    RAY_LOG(DEBUG) << "Generator backpressured, consumed: " << total_objects_consumed_
                   << ". generated: " << total_objects_generated_
                   << ". threshold: " << backpressure_threshold_;
    while (total_object_unconsumed >= backpressure_threshold_) {
      backpressure_cond_var_.WaitWithTimeout(&mutex_, absl::Seconds(1));
      total_object_unconsumed = total_objects_generated_ - total_objects_consumed_;
      return_status = check_signals_();
      if (!return_status.ok()) {
        break;
      }
    }
  }
  return return_status;
}

Status TaskGeneratorBackpressureWaiter::WaitAllObjectsReported() {
  absl::MutexLock lock(&mutex_);
  auto return_status = Status::OK();
  while (num_object_reports_in_flight_ > 0) {
    all_objects_reported_cond_var_.WaitWithTimeout(&mutex_, absl::Seconds(1));
    return_status = check_signals_();
    if (!return_status.ok()) {
      break;
    }
  }
  return return_status;
}

void TaskGeneratorBackpressureWaiter::IncrementObjectGenerated() {
  absl::MutexLock lock(&mutex_);
  total_objects_generated_ += 1;
  num_object_reports_in_flight_++;
}

void TaskGeneratorBackpressureWaiter::HandleObjectReported(
    int64_t total_objects_consumed) {
  absl::MutexLock lock(&mutex_);
  num_object_reports_in_flight_--;
  if (num_object_reports_in_flight_ < 0) {
    RAY_LOG(INFO)
        << "Streaming generator executor received more object report acks than sent. If "
           "the worker dies after finishing the task and some object reports have not "
           "been acked yet, then the consumer may hang when trying to get those objects.";
  }
  if (num_object_reports_in_flight_ <= 0) {
    all_objects_reported_cond_var_.SignalAll();
  }

  total_objects_consumed_ = std::max(total_objects_consumed_, total_objects_consumed);
  auto total_object_unconsumed = total_objects_generated_ - total_objects_consumed_;
  if (total_object_unconsumed < backpressure_threshold_) {
    backpressure_cond_var_.SignalAll();
  }
}

int64_t TaskGeneratorBackpressureWaiter::TotalObjectConsumed() const {
  absl::MutexLock lock(&mutex_);
  return total_objects_consumed_;
}

int64_t TaskGeneratorBackpressureWaiter::TotalObjectGenerated() const {
  absl::MutexLock lock(&mutex_);
  return total_objects_generated_;
}

ActorWideGeneratorBackpressureWaiter::ActorWideGeneratorBackpressureWaiter(
    int64_t actor_cap, std::function<Status()> check_signals)
    : backpressure_threshold_(actor_cap), check_signals_(std::move(check_signals)) {
  RAY_CHECK_GT(backpressure_threshold_, 0);
  RAY_CHECK(check_signals_ != nullptr);
}

Status ActorWideGeneratorBackpressureWaiter::ReserveActorWideSlot(
    ActorTaskBackpressureMetadata &metadata) {
  absl::MutexLock lock(&mutex_);
  while (total_objects_generated_ - total_objects_consumed_ >= backpressure_threshold_) {
    backpressure_cond_var_.WaitWithTimeout(&mutex_, absl::Seconds(1));
    auto status = check_signals_();
    if (!status.ok()) {
      return status;
    }
  }
  total_objects_generated_ += 1;
  metadata.per_task_generated += 1;
  return Status::OK();
}

void ActorWideGeneratorBackpressureWaiter::OnReportForTask(
    ActorTaskBackpressureMetadata &metadata, int64_t total) {
  absl::MutexLock lock(&mutex_);
  if (!metadata.task_alive) {
    return;
  }
  // per_task_generated counts ReserveActorWideSlot admissions only; reported
  // totals may not line up (e.g. substitute values on RPC failure).
  const int64_t clamped_total = std::min(total, metadata.per_task_generated);
  int64_t delta = clamped_total - metadata.per_task_consumed;
  if (delta <= 0) {
    return;
  }
  metadata.per_task_consumed = clamped_total;
  total_objects_consumed_ += delta;
  if (total_objects_generated_ - total_objects_consumed_ < backpressure_threshold_) {
    backpressure_cond_var_.SignalAll();
  }
}

void ActorWideGeneratorBackpressureWaiter::TeardownTask(
    ActorTaskBackpressureMetadata &metadata) {
  absl::MutexLock lock(&mutex_);
  if (!metadata.task_alive) {
    return;
  }
  metadata.task_alive = false;
  int64_t outstanding = metadata.per_task_generated - metadata.per_task_consumed;
  if (outstanding > 0) {
    total_objects_generated_ -= outstanding;
    if (total_objects_generated_ - total_objects_consumed_ < backpressure_threshold_) {
      backpressure_cond_var_.SignalAll();
    }
  }
}

int64_t ActorWideGeneratorBackpressureWaiter::TotalObjectConsumed() const {
  absl::MutexLock lock(&mutex_);
  return total_objects_consumed_;
}

int64_t ActorWideGeneratorBackpressureWaiter::TotalObjectGenerated() const {
  absl::MutexLock lock(&mutex_);
  return total_objects_generated_;
}

Status ActorTaskBackpressureMetadata::ReserveSlot() {
  return actor_waiter->ReserveActorWideSlot(*this);
}

void ActorTaskBackpressureMetadata::OnReport(int64_t total) {
  actor_waiter->OnReportForTask(*this, total);
}

void ActorTaskBackpressureMetadata::Teardown() { actor_waiter->TeardownTask(*this); }

}  // namespace core
}  // namespace ray
