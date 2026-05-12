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
/// ReserveActorWideSlot, OnReportForTask, and TeardownTask on that waiter.
struct ActorTaskBackpressureMetadata {
  std::shared_ptr<ActorWideGeneratorBackpressureWaiter> actor_waiter;
  int64_t per_task_generated = 0;
  int64_t per_task_consumed = 0;
  bool task_alive = true;

  explicit ActorTaskBackpressureMetadata(
      std::shared_ptr<ActorWideGeneratorBackpressureWaiter> w)
      : actor_waiter(std::move(w)) {}

  // Thin forwarders for Cython and RPC callbacks.
  Status ReserveSlot();
  void OnReport(int64_t total);
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
  Status WaitAllObjectsReported();

  void IncrementObjectGenerated();
  void HandleObjectReported(int64_t total_objects_consumed);

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
};

/// Shared across all streaming-generator tasks on one actor; enforces
/// `_actor_generator_backpressure_num_objects`.
class ActorWideGeneratorBackpressureWaiter {
 public:
  /// \param[in] actor_cap Must be > 0 (callers create this only when the actor
  /// option is set).
  ActorWideGeneratorBackpressureWaiter(int64_t actor_cap,
                                       std::function<Status()> check_signals);

  Status ReserveActorWideSlot(ActorTaskBackpressureMetadata &metadata);
  void OnReportForTask(ActorTaskBackpressureMetadata &metadata, int64_t total);
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
