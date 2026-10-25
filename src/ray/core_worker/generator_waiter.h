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
  void OnConsumed(int64_t total);
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
/// `_actor_generator_backpressure_num_objects`.
class ActorWideGeneratorBackpressureWaiter {
 public:
  /// \param[in] actor_cap Must be > 0 (callers create this only when the actor
  /// option is set).
  ActorWideGeneratorBackpressureWaiter(int64_t actor_cap,
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
  void OnConsumedForTask(ActorTaskBackpressureMetadata &metadata, int64_t total);
  void TeardownTask(ActorTaskBackpressureMetadata &metadata);

  int64_t TotalObjectConsumed() const;
  int64_t TotalObjectGenerated() const;

 private:
  mutable absl::Mutex mutex_;
  absl::CondVar backpressure_cond_var_;
  const int64_t backpressure_threshold_;
  const std::function<Status()> check_signals_;
  int64_t total_objects_generated_ = 0;
  int64_t total_objects_consumed_ = 0;
};

}  // namespace core
}  // namespace ray
