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

  mutex_.Lock();

  auto return_status = Status::OK();
  auto total_object_unconsumed = total_objects_generated_ - total_objects_consumed_;
  if (!backpressure_disabled_ && total_object_unconsumed >= backpressure_threshold_) {
    RAY_LOG(DEBUG) << "Generator backpressured, consumed: " << total_objects_consumed_
                   << ". generated: " << total_objects_generated_
                   << ". threshold: " << backpressure_threshold_;
    while (!backpressure_disabled_ &&
           total_object_unconsumed >= backpressure_threshold_) {
      backpressure_cond_var_.WaitWithTimeout(&mutex_, absl::Seconds(1));
      // Release before check_signals(): it acquires the GIL, and callers may take
      // this mutex while holding the GIL (async generators). Opposite order
      // deadlocks.
      mutex_.Unlock();
      return_status = check_signals_();
      mutex_.Lock();
      if (!return_status.ok()) {
        break;
      }
      // Re-read under the lock; consumption may have progressed while unlocked.
      total_object_unconsumed = total_objects_generated_ - total_objects_consumed_;
    }
  }
  mutex_.Unlock();
  return return_status;
}

bool TaskGeneratorBackpressureWaiter::IsBackpressured() const {
  if (backpressure_threshold_ < 0) {
    return false;
  }
  absl::MutexLock lock(&mutex_);
  return !backpressure_disabled_ &&
         total_objects_generated_ - total_objects_consumed_ >= backpressure_threshold_;
}

Status TaskGeneratorBackpressureWaiter::WaitAllObjectsReported() {
  mutex_.Lock();
  auto return_status = Status::OK();
  while (!backpressure_disabled_ && num_object_reports_in_flight_ > 0) {
    all_objects_reported_cond_var_.WaitWithTimeout(&mutex_, absl::Seconds(1));
    // See WaitUntilObjectConsumed: never call check_signals while holding mutex_.
    mutex_.Unlock();
    return_status = check_signals_();
    mutex_.Lock();
    if (!return_status.ok()) {
      break;
    }
  }
  mutex_.Unlock();
  return return_status;
}

void TaskGeneratorBackpressureWaiter::DisableBackpressure() {
  absl::MutexLock lock(&mutex_);
  backpressure_disabled_ = true;
  backpressure_cond_var_.SignalAll();
  all_objects_reported_cond_var_.SignalAll();
}

void TaskGeneratorBackpressureWaiter::IncrementObjectGenerated(
    int64_t num_objects_generated) {
  RAY_CHECK_GE(num_objects_generated, 0);
  absl::MutexLock lock(&mutex_);
  total_objects_generated_ += num_objects_generated;
  num_object_reports_in_flight_++;
}

void TaskGeneratorBackpressureWaiter::OnObjectReportAccepted() {
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
}

void TaskGeneratorBackpressureWaiter::OnObjectConsumed(int64_t total_objects_consumed) {
  absl::MutexLock lock(&mutex_);
  total_objects_consumed_ = std::max(total_objects_consumed_, total_objects_consumed);
  auto total_object_unconsumed = total_objects_generated_ - total_objects_consumed_;
  if (total_object_unconsumed < backpressure_threshold_) {
    backpressure_cond_var_.SignalAll();
  }
}

bool TaskGeneratorBackpressureWaiter::NeedsObjectConsumedUpdates() const {
  return backpressure_threshold_ > 0;
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
    ActorTaskBackpressureMetadata &metadata, int64_t num_objects) {
  mutex_.Lock();
  // Wait until the shared budget is below the cap, then admit the whole group
  // of `num_objects`. A single group is admitted even if it overshoots the cap
  // (mirrors the per-task waiter, which reports a full grouped yield before
  // blocking); otherwise a group larger than the cap could never make progress.
  while (metadata.task_alive &&
         total_objects_generated_ - total_objects_consumed_ >= backpressure_threshold_) {
    backpressure_cond_var_.WaitWithTimeout(&mutex_, absl::Seconds(1));
    // Release before check_signals(): it acquires the GIL. Async generators call
    // TryReserveSlot / IsBackpressured while holding the GIL, so holding both
    // locks in opposite order deadlocks (mutex_ then GIL vs GIL then mutex_).
    mutex_.Unlock();
    auto status = check_signals_();
    mutex_.Lock();
    if (!status.ok()) {
      mutex_.Unlock();
      return status;
    }
  }
  if (!metadata.task_alive) {
    mutex_.Unlock();
    return Status::OK();
  }
  total_objects_generated_ += num_objects;
  metadata.per_task_generated += num_objects;
  mutex_.Unlock();
  return Status::OK();
}

bool ActorWideGeneratorBackpressureWaiter::TryReserveActorWideSlot(
    ActorTaskBackpressureMetadata &metadata, int64_t num_objects) {
  absl::MutexLock lock(&mutex_);
  if (!metadata.task_alive) {
    // Mirror ReserveActorWideSlot, which returns OK without admitting when the
    // task is no longer alive. The caller stops streaming shortly after.
    return true;
  }
  if (total_objects_generated_ - total_objects_consumed_ >= backpressure_threshold_) {
    return false;
  }
  total_objects_generated_ += num_objects;
  metadata.per_task_generated += num_objects;
  return true;
}

void ActorWideGeneratorBackpressureWaiter::ReleaseActorWideSlot(
    ActorTaskBackpressureMetadata &metadata, int64_t num_objects) {
  absl::MutexLock lock(&mutex_);
  if (!metadata.task_alive) {
    return;
  }
  const int64_t releasable =
      std::min(num_objects, metadata.per_task_generated - metadata.per_task_consumed);
  if (releasable <= 0) {
    return;
  }
  metadata.per_task_generated -= releasable;
  total_objects_generated_ -= releasable;
  if (total_objects_generated_ - total_objects_consumed_ < backpressure_threshold_) {
    backpressure_cond_var_.SignalAll();
  }
}

void ActorWideGeneratorBackpressureWaiter::OnConsumedForTask(
    ActorTaskBackpressureMetadata &metadata, int64_t total) {
  absl::MutexLock lock(&mutex_);
  if (!metadata.task_alive) {
    return;
  }
  // per_task_generated counts ReserveActorWideSlot admissions in object units
  // (matching the owner-reported consumed total); reported totals may still not
  // line up (e.g. substitute values on RPC failure), so clamp to what we admitted.
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
  }
  // Always signal so any task parked in ReserveActorWideSlot (per_task_generated
  // could still be 0 if it never got to admit anything) wakes up and rechecks
  // task_alive immediately rather than waiting on the 1s WaitWithTimeout tick.
  backpressure_cond_var_.SignalAll();
}

int64_t ActorWideGeneratorBackpressureWaiter::TotalObjectConsumed() const {
  absl::MutexLock lock(&mutex_);
  return total_objects_consumed_;
}

int64_t ActorWideGeneratorBackpressureWaiter::TotalObjectGenerated() const {
  absl::MutexLock lock(&mutex_);
  return total_objects_generated_;
}

Status ActorTaskBackpressureMetadata::ReserveSlot(int64_t num_objects) {
  return actor_waiter->ReserveActorWideSlot(*this, num_objects);
}

bool ActorTaskBackpressureMetadata::TryReserveSlot(int64_t num_objects) {
  return actor_waiter->TryReserveActorWideSlot(*this, num_objects);
}

void ActorTaskBackpressureMetadata::ReleaseSlot(int64_t num_objects) {
  actor_waiter->ReleaseActorWideSlot(*this, num_objects);
}

void ActorTaskBackpressureMetadata::OnConsumed(int64_t total) {
  actor_waiter->OnConsumedForTask(*this, total);
}

void ActorTaskBackpressureMetadata::Teardown() { actor_waiter->TeardownTask(*this); }

}  // namespace core
}  // namespace ray
