// Copyright 2020-2021 The Ray Authors.
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

#include <deque>
#include <memory>
#include <string>

#include "absl/container/flat_hash_map.h"
#include "ray/common/task/task.h"
#include "ray/raylet/scheduling/internal.h"

namespace ray {
namespace raylet {

// Forward declaration
class WorkerInterface;

/// Manages the lifetime of a task on the local node. It receives request from
/// cluster_task_manager and tries to execute the task locally.
/// Read raylet/local_task_manager.h for more information.
class LocalTaskManagerInterface {
 public:
  virtual ~LocalTaskManagerInterface() = default;

  /// Queue task and schedule.
  virtual void QueueAndScheduleLease(std::shared_ptr<internal::Work> work) = 0;

  // Schedule and dispatch tasks.
  virtual void ScheduleAndDispatchLeases() = 0;

  /// Attempt to cancel all queued tasks that match the predicate.
  ///
  /// \param predicate: A function that returns true if a task needs to be cancelled.
  /// \param failure_type: The reason for cancellation.
  /// \param scheduling_failure_message: The reason message for cancellation.
  /// \return True if any task was successfully cancelled.
  virtual bool CancelLeases(
      std::function<bool(const std::shared_ptr<internal::Work> &)> predicate,
      rpc::RequestWorkerLeaseReply::SchedulingFailureType failure_type,
      const std::string &scheduling_failure_message) = 0;

  virtual const absl::flat_hash_map<SchedulingClass,
                                    std::deque<std::shared_ptr<internal::Work>>>
      &GetLeaseToDispatch() const = 0;

  virtual const absl::flat_hash_map<SchedulingClass,
                                    absl::flat_hash_map<WorkerID, int64_t>>
      &GetBackLogTracker() const = 0;

  virtual void SetWorkerBacklog(SchedulingClass scheduling_class,
                                const WorkerID &worker_id,
                                int64_t backlog_size) = 0;

  virtual void ClearWorkerBacklog(const WorkerID &worker_id) = 0;

  virtual const RayTask *AnyPendingLeasesForResourceAcquisition(
      int *num_pending_actor_creation, int *num_pending_leases) const = 0;

  virtual void LeasesUnblocked(const std::vector<LeaseID> &ready_ids) = 0;

  virtual void LeaseFinished(std::shared_ptr<WorkerInterface> worker, RayTask *lease) = 0;

  virtual void ReleaseWorkerResources(std::shared_ptr<WorkerInterface> worker) = 0;

  virtual bool ReleaseCpuResourcesFromBlockedWorker(
      std::shared_ptr<WorkerInterface> worker) = 0;

  virtual bool ReturnCpuResourcesToUnblockedWorker(
      std::shared_ptr<WorkerInterface> worker) = 0;

  virtual ResourceSet CalcNormalLeaseResources() const = 0;

  virtual void RecordMetrics() const = 0;

  virtual void DebugStr(std::stringstream &buffer) const = 0;

  virtual size_t GetNumLeaseSpilled() const = 0;
  virtual size_t GetNumWaitingLeaseSpilled() const = 0;
  virtual size_t GetNumUnschedulableLeaseSpilled() const = 0;
};

/// A noop local task manager. It is a no-op class. We need this because there's no
/// "LocalTaskManager" when the `ClusterTaskManager` is used within GCS. In the long term,
/// we should make `ClusterTaskManager` not aware of `LocalTaskManager`.
class NoopLocalTaskManager : public LocalTaskManagerInterface {
 public:
  NoopLocalTaskManager() = default;

  /// Queue lease and schedule.
  void QueueAndScheduleLease(std::shared_ptr<internal::Work> work) override {
    RAY_CHECK(false)
        << "This function should never be called by gcs' local task manager.";
  }

  // Schedule and dispatch leases.
  void ScheduleAndDispatchLeases() override {}

  bool CancelLeases(
      std::function<bool(const std::shared_ptr<internal::Work> &)> predicate,
      rpc::RequestWorkerLeaseReply::SchedulingFailureType failure_type,
      const std::string &scheduling_failure_message) override {
    return false;
  }

  const absl::flat_hash_map<SchedulingClass, std::deque<std::shared_ptr<internal::Work>>>
      &GetLeaseToDispatch() const override {
    static const absl::flat_hash_map<SchedulingClass,
                                     std::deque<std::shared_ptr<internal::Work>>>
        leases_to_dispatch;
    return leases_to_dispatch;
  }

  const absl::flat_hash_map<SchedulingClass, absl::flat_hash_map<WorkerID, int64_t>>
      &GetBackLogTracker() const override {
    static const absl::flat_hash_map<SchedulingClass,
                                     absl::flat_hash_map<WorkerID, int64_t>>
        backlog_tracker;
    return backlog_tracker;
  }

  void SetWorkerBacklog(SchedulingClass scheduling_class,
                        const WorkerID &worker_id,
                        int64_t backlog_size) override {}

  void ClearWorkerBacklog(const WorkerID &worker_id) override {}

  const RayTask *AnyPendingLeasesForResourceAcquisition(
      int *num_pending_actor_creation, int *num_pending_leases) const override {
    return nullptr;
  }

  void LeasesUnblocked(const std::vector<LeaseID> &ready_ids) override {}

  void LeaseFinished(std::shared_ptr<WorkerInterface> worker, RayTask *lease) override {}

  void ReleaseWorkerResources(std::shared_ptr<WorkerInterface> worker) override {}

  bool ReleaseCpuResourcesFromBlockedWorker(
      std::shared_ptr<WorkerInterface> worker) override {
    return false;
  }

  bool ReturnCpuResourcesToUnblockedWorker(
      std::shared_ptr<WorkerInterface> worker) override {
    return false;
  }

  ResourceSet CalcNormalLeaseResources() const override { return ResourceSet(); }

  void RecordMetrics() const override{};

  void DebugStr(std::stringstream &buffer) const override {}

  size_t GetNumLeaseSpilled() const override { return 0; }
  size_t GetNumWaitingLeaseSpilled() const override { return 0; }
  size_t GetNumUnschedulableLeaseSpilled() const override { return 0; }
};

}  // namespace raylet
}  // namespace ray
