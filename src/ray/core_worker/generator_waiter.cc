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
  if (!backpressure_disabled_ && total_object_unconsumed >= backpressure_threshold_) {
    RAY_LOG(DEBUG) << "Generator backpressured, consumed: " << total_objects_consumed_
                   << ". generated: " << total_objects_generated_
                   << ". threshold: " << backpressure_threshold_;
    while (!backpressure_disabled_ &&
           total_object_unconsumed >= backpressure_threshold_) {
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

bool TaskGeneratorBackpressureWaiter::IsBackpressured() const {
  if (backpressure_threshold_ < 0) {
    return false;
  }
  absl::MutexLock lock(&mutex_);
  return !backpressure_disabled_ &&
         total_objects_generated_ - total_objects_consumed_ >= backpressure_threshold_;
}

Status TaskGeneratorBackpressureWaiter::WaitAllObjectsReported() {
  absl::MutexLock lock(&mutex_);
  auto return_status = Status::OK();
  while (!backpressure_disabled_ && num_object_reports_in_flight_ > 0) {
    all_objects_reported_cond_var_.WaitWithTimeout(&mutex_, absl::Seconds(1));
    return_status = check_signals_();
    if (!return_status.ok()) {
      break;
    }
  }
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
    int64_t actor_object_cap,
    int64_t actor_byte_cap,
    int64_t max_object_bytes,
    bool predict_object_bytes,
    std::function<Status()> check_signals)
    : object_backpressure_threshold_(actor_object_cap),
      byte_backpressure_threshold_(actor_byte_cap),
      max_object_bytes_(max_object_bytes),
      predict_object_bytes_(predict_object_bytes),
      check_signals_(std::move(check_signals)) {
  RAY_CHECK(object_backpressure_threshold_ > 0 || byte_backpressure_threshold_ > 0);
  RAY_CHECK(check_signals_ != nullptr);
}

int64_t ActorWideGeneratorBackpressureWaiter::BoundPerObject() const {
  if (!predict_object_bytes_) {
    return 0;
  }
  if (cumulative_reported_objects_ > 0) {
    return cumulative_reported_bytes_ / cumulative_reported_objects_;
  }
  // No report yet; fall back to the caller-declared seed, if any. The seed is
  // deliberately superseded by the observed average as soon as real sizes are
  // known: a conservative seed (e.g. a max block size much larger than typical
  // blocks) must not throttle the stream permanently.
  return std::max<int64_t>(max_object_bytes_, 0);
}

bool ActorWideGeneratorBackpressureWaiter::BudgetExhausted(int64_t num_objects) const {
  if (object_backpressure_threshold_ > 0 &&
      total_objects_generated_ - total_objects_consumed_ >=
          object_backpressure_threshold_) {
    return true;
  }
  if (byte_backpressure_threshold_ > 0) {
    const int64_t bytes_outstanding = total_bytes_generated_ - total_bytes_consumed_;
    // With nothing outstanding at all (no unconsumed bytes and no
    // admitted-but-unreported yields), always admit so a per-object estimate
    // at or above the cap can never deadlock the actor. Without prediction
    // the estimate is 0 and this guard is a no-op.
    const bool nothing_outstanding = bytes_outstanding == 0 && inflight_objects_ == 0;
    if (!nothing_outstanding &&
        bytes_outstanding + BoundPerObject() * (inflight_objects_ + num_objects) >=
            byte_backpressure_threshold_) {
      return true;
    }
  }
  return false;
}

Status ActorWideGeneratorBackpressureWaiter::ReserveActorWideSlot(
    ActorTaskBackpressureMetadata &metadata, int64_t num_objects) {
  absl::MutexLock lock(&mutex_);
  // Wait until the shared budgets are below their caps, then admit the whole
  // group of `num_objects`. A single group is admitted even if it overshoots a
  // cap (mirrors the per-task waiter, which reports a full grouped yield before
  // blocking); otherwise a group larger than the cap could never make progress.
  while (metadata.task_alive && BudgetExhausted(num_objects)) {
    backpressure_cond_var_.WaitWithTimeout(&mutex_, absl::Seconds(1));
    auto status = check_signals_();
    if (!status.ok()) {
      return status;
    }
  }
  if (!metadata.task_alive) {
    return Status::OK();
  }
  total_objects_generated_ += num_objects;
  metadata.per_task_generated += num_objects;
  inflight_objects_ += num_objects;
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
  if (BudgetExhausted(num_objects)) {
    return false;
  }
  total_objects_generated_ += num_objects;
  metadata.per_task_generated += num_objects;
  inflight_objects_ += num_objects;
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
  // Released yields produced no object, so their bytes were never charged;
  // drop them from the in-flight count.
  inflight_objects_ = std::max<int64_t>(inflight_objects_ - releasable, 0);
  if (!BudgetExhausted(1)) {
    backpressure_cond_var_.SignalAll();
  }
}

void ActorWideGeneratorBackpressureWaiter::AddBytesGeneratedForTask(
    ActorTaskBackpressureMetadata &metadata, int64_t num_objects, int64_t num_bytes) {
  RAY_CHECK_GE(num_objects, 0);
  RAY_CHECK_GE(num_bytes, 0);
  absl::MutexLock lock(&mutex_);
  if (!metadata.task_alive) {
    return;
  }
  total_bytes_generated_ += num_bytes;
  metadata.per_task_generated_bytes += num_bytes;
  metadata.per_task_byte_charged_objects += num_objects;
  cumulative_reported_objects_ += num_objects;
  cumulative_reported_bytes_ += num_bytes;
  inflight_objects_ = std::max<int64_t>(inflight_objects_ - num_objects, 0);
  // With prediction on, an actual size below the current estimate can free
  // predicted budget a parked reserver was waiting on.
  if (!BudgetExhausted(1)) {
    backpressure_cond_var_.SignalAll();
  }
}

void ActorWideGeneratorBackpressureWaiter::OnConsumedForTask(
    ActorTaskBackpressureMetadata &metadata, int64_t total_objects, int64_t total_bytes) {
  absl::MutexLock lock(&mutex_);
  if (!metadata.task_alive) {
    return;
  }
  // per_task_generated counts ReserveActorWideSlot admissions in object units
  // (matching the owner-reported consumed total); reported totals may still not
  // line up (e.g. substitute values on RPC failure), so clamp to what we admitted.
  // Objects and bytes advance independently: a stale ack can regress one
  // dimension but not the other.
  const int64_t clamped_total = std::min(total_objects, metadata.per_task_generated);
  const int64_t delta = clamped_total - metadata.per_task_consumed;
  if (delta > 0) {
    metadata.per_task_consumed = clamped_total;
    total_objects_consumed_ += delta;
  }
  const int64_t clamped_total_bytes =
      std::min(total_bytes, metadata.per_task_generated_bytes);
  const int64_t byte_delta = clamped_total_bytes - metadata.per_task_consumed_bytes;
  if (byte_delta > 0) {
    metadata.per_task_consumed_bytes = clamped_total_bytes;
    total_bytes_consumed_ += byte_delta;
  }
  if (delta <= 0 && byte_delta <= 0) {
    return;
  }
  if (!BudgetExhausted(1)) {
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
  const int64_t outstanding_bytes =
      metadata.per_task_generated_bytes - metadata.per_task_consumed_bytes;
  if (outstanding_bytes > 0) {
    total_bytes_generated_ -= outstanding_bytes;
  }
  // Objects this task admitted but never byte-charged will never report.
  const int64_t task_inflight =
      metadata.per_task_generated - metadata.per_task_byte_charged_objects;
  if (task_inflight > 0) {
    inflight_objects_ = std::max<int64_t>(inflight_objects_ - task_inflight, 0);
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

int64_t ActorWideGeneratorBackpressureWaiter::TotalBytesConsumed() const {
  absl::MutexLock lock(&mutex_);
  return total_bytes_consumed_;
}

int64_t ActorWideGeneratorBackpressureWaiter::TotalBytesGenerated() const {
  absl::MutexLock lock(&mutex_);
  return total_bytes_generated_;
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

void ActorTaskBackpressureMetadata::AddBytesGenerated(int64_t num_objects,
                                                      int64_t num_bytes) {
  actor_waiter->AddBytesGeneratedForTask(*this, num_objects, num_bytes);
}

void ActorTaskBackpressureMetadata::OnConsumed(int64_t total_objects,
                                               int64_t total_bytes) {
  actor_waiter->OnConsumedForTask(*this, total_objects, total_bytes);
}

void ActorTaskBackpressureMetadata::Teardown() { actor_waiter->TeardownTask(*this); }

}  // namespace core
}  // namespace ray
