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

#include <memory>
#include <utility>

#include "absl/synchronization/mutex.h"
#include "ray/core_worker/common.h"

namespace ray {
namespace core {

class ActorWideGeneratorBackpressureWaiter;

/// Per-(streaming-generator)-task counters and liveness for the actor-wide
/// waiter: one running generator task contributes its share of the shared
/// unconsumed-object budget.
///
/// Fields are guarded by the owning waiter's mutex_. Mutations go only through
/// ReserveActorWideSlot, ReleaseActorWideSlot, OnConsumedForTask, and TeardownTask
/// on that waiter.
struct ActorTaskBackpressureMetadata {
  std::shared_ptr<ActorWideGeneratorBackpressureWaiter> actor_waiter;
  int64_t per_task_generated = 0;
  int64_t per_task_consumed = 0;
  /// Bytes of this task's reported objects (charged at report time, when
  /// object sizes are first known).
  int64_t per_task_generated_bytes = 0;
  /// Bytes the owner has acknowledged as consumed for this task.
  int64_t per_task_consumed_bytes = 0;
  /// Objects of this task whose actual bytes have been charged via
  /// AddBytesGenerated. per_task_generated - per_task_byte_charged_objects is
  /// this task's contribution to the waiter's in-flight object count.
  int64_t per_task_byte_charged_objects = 0;
  bool task_alive = true;

  explicit ActorTaskBackpressureMetadata(
      std::shared_ptr<ActorWideGeneratorBackpressureWaiter> w)
      : actor_waiter(std::move(w)) {}

  // Thin forwarders for Cython and RPC callbacks.
  // num_objects is the number of objects the yield produces
  // (`_num_objects_per_yield`), so a grouped yield reserves/releases its whole
  // group of objects against the actor-wide budget.
  Status ReserveSlot(int64_t num_objects = 1);
  /**
   * @brief Non-blocking variant of ReserveSlot used by async streaming
   * generators.
   *
   * The caller awaits an asyncio.Event instead of blocking a thread when it must
   * wait for budget.
   *
   * @param[in] num_objects Number of objects the yield produces
   * (`_num_objects_per_yield`); the whole group is admitted at once.
   * @return True if the group was admitted against the actor-wide budget (or the
   * task is no longer alive, so the caller should stop); false if the caller must
   * wait for budget.
   */
  bool TryReserveSlot(int64_t num_objects = 1);
  void ReleaseSlot(int64_t num_objects = 1);
  /// Charge the actual bytes of num_objects reported objects against the
  /// actor-wide byte budget. Called at report time, when sizes are known.
  void AddBytesGenerated(int64_t num_objects, int64_t num_bytes);
  void OnConsumed(int64_t total_objects, int64_t total_bytes);
  void Teardown();
};

/// Per streaming generator task: owner RPC reporting and per-task unconsumed cap.
class TaskGeneratorBackpressureWaiter {
 public:
  /// \param[in] generator_backpressure_num_objects Same semantics as
  /// TaskSpecification::GeneratorBackpressureNumObjects (-1 disables).
  /// \param[in] check_signals Invoked periodically while blocked.
  TaskGeneratorBackpressureWaiter(int64_t generator_backpressure_num_objects,
                                  std::function<Status()> check_signals);

  Status WaitUntilObjectConsumed();
  /**
   * @brief Non-blocking check used by async streaming generators to decide
   * whether to pause.
   *
   * Lets the async executor await an asyncio.Event instead of blocking a thread
   * in WaitUntilObjectConsumed.
   *
   * @return True if the per-task unconsumed-object count is at/above the
   * threshold (the generator should pause); false when backpressure is disabled
   * or the threshold (-1) is not configured.
   */
  bool IsBackpressured() const;
  Status WaitAllObjectsReported();

  /// Increment the number of objects generated. The executor should call this
  /// before sending an object report to the caller.
  void IncrementObjectGenerated(int64_t num_objects_generated = 1);
  void OnObjectReportAccepted();
  void OnObjectConsumed(int64_t total_objects_consumed);

  /// Permanently disable backpressure for this waiter. After this is called,
  /// WaitUntilObjectConsumed returns immediately and WaitAllObjectsReported
  /// stops waiting on outstanding report acks.
  void DisableBackpressure();

  bool NeedsObjectConsumedUpdates() const;
  int64_t TotalObjectConsumed() const;
  int64_t TotalObjectGenerated() const;

 private:
  mutable absl::Mutex mutex_;
  absl::CondVar backpressure_cond_var_;
  absl::CondVar all_objects_reported_cond_var_;
  const int64_t backpressure_threshold_;
  const std::function<Status()> check_signals_;
  int64_t total_objects_generated_ = 0;
  int64_t num_object_reports_in_flight_ = 0;
  int64_t total_objects_consumed_ = 0;
  bool backpressure_disabled_ = false;
};

/// Shared across all streaming-generator tasks on one actor; enforces
/// `_actor_generator_backpressure_num_objects` and/or
/// `_actor_generator_backpressure_num_bytes`. When both caps are set, a yield
/// blocks while either budget is exhausted.
class ActorWideGeneratorBackpressureWaiter {
 public:
  /// \param[in] actor_object_cap Actor-wide unconsumed-object cap; <= 0
  /// disables the object budget.
  /// \param[in] actor_byte_cap Actor-wide unconsumed-byte cap; <= 0 disables
  /// the byte budget. At least one of the two caps must be > 0.
  /// \param[in] max_object_bytes Cold-start seed for the byte-size estimator
  /// (upper bound on a single yielded object's size); <= 0 leaves the
  /// estimator unseeded. Only used when predict_object_bytes is true.
  /// \param[in] predict_object_bytes If true, gate admissions on the estimated
  /// bytes of admitted-but-unreported yields (running average of reported
  /// object sizes, seeded by max_object_bytes before the first report) so the
  /// byte cap is enforced before generating instead of one yield late.
  ActorWideGeneratorBackpressureWaiter(int64_t actor_object_cap,
                                       int64_t actor_byte_cap,
                                       int64_t max_object_bytes,
                                       bool predict_object_bytes,
                                       std::function<Status()> check_signals);

  // num_objects is the number of objects admitted/reclaimed in one call so the
  // actor-wide budget is accounted in object units even when a single yield
  // produces multiple objects (`_num_objects_per_yield` > 1).
  Status ReserveActorWideSlot(ActorTaskBackpressureMetadata &metadata,
                              int64_t num_objects = 1);
  /**
   * @brief Non-blocking variant of ReserveActorWideSlot (see
   * ActorTaskBackpressureMetadata::TryReserveSlot).
   *
   * @param[in] metadata Per-task accounting for the calling generator task.
   * @param[in] num_objects Number of objects to admit in one call, in object
   * units (so a grouped yield reserves its whole group).
   * @return True if admitted (or the task is no longer alive); false if the
   * caller must wait for budget.
   */
  bool TryReserveActorWideSlot(ActorTaskBackpressureMetadata &metadata,
                               int64_t num_objects = 1);
  void ReleaseActorWideSlot(ActorTaskBackpressureMetadata &metadata,
                            int64_t num_objects = 1);
  /// Charge the actual bytes of num_objects of the task's reported objects
  /// against the shared byte budget. Called at report time (sizes are unknown
  /// at reserve time), so it never blocks; the byte budget is enforced by the
  /// next reserve.
  void AddBytesGeneratedForTask(ActorTaskBackpressureMetadata &metadata,
                                int64_t num_objects,
                                int64_t num_bytes);
  void OnConsumedForTask(ActorTaskBackpressureMetadata &metadata,
                         int64_t total_objects,
                         int64_t total_bytes);
  void TeardownTask(ActorTaskBackpressureMetadata &metadata);

  int64_t TotalObjectConsumed() const;
  int64_t TotalObjectGenerated() const;
  int64_t TotalBytesConsumed() const;
  int64_t TotalBytesGenerated() const;

 private:
  /// Whether admitting num_objects more objects would exceed either budget.
  ///
  /// The byte term compares outstanding bytes plus an estimate for
  /// admitted-but-unreported objects (see BoundPerObject) against the byte
  /// cap. When nothing is outstanding (no unconsumed bytes and no in-flight
  /// yields) the byte budget always admits, so an estimate at or above the
  /// cap can never deadlock the actor; without prediction the estimate is 0
  /// and the guard is a no-op.
  bool BudgetExhausted(int64_t num_objects) const ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  /// Estimated bytes of one admitted-but-unreported object: the running
  /// average of reported object sizes, or the max_object_bytes_ seed before
  /// the first report. 0 when prediction is disabled.
  int64_t BoundPerObject() const ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  mutable absl::Mutex mutex_;
  absl::CondVar backpressure_cond_var_;
  const int64_t object_backpressure_threshold_;
  const int64_t byte_backpressure_threshold_;
  const int64_t max_object_bytes_;
  const bool predict_object_bytes_;
  const std::function<Status()> check_signals_;
  int64_t total_objects_generated_ = 0;
  int64_t total_objects_consumed_ = 0;
  int64_t total_bytes_generated_ = 0;
  int64_t total_bytes_consumed_ = 0;
  /// Objects admitted at reserve time whose actual bytes have not been
  /// charged yet, so concurrent streams see each other's pending yields.
  int64_t inflight_objects_ = 0;
  /// Lifetime totals of reported objects/bytes for the size estimator; unlike
  /// total_bytes_generated_, never reclaimed by release or teardown.
  int64_t cumulative_reported_objects_ = 0;
  int64_t cumulative_reported_bytes_ = 0;
};

}  // namespace core
}  // namespace ray
