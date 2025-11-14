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
#include "ray/raylet/scheduling/internal.h"

namespace ray {
class RayLease;

namespace raylet {

// Forward declaration
class WorkerInterface;

/// Manages the lifetime of a lease on the local node. It receives request from
/// cluster_lease_manager and tries to execute the lease locally.
/// Read raylet/local_lease_manager.h for more information.
class LocalLeaseManagerInterface {
 public:
  virtual ~LocalLeaseManagerInterface() = default;

  /// Queue lease and schedule.
  virtual void QueueAndScheduleLease(std::shared_ptr<internal::Work> work) = 0;

  // Schedule and grant leases.
  virtual void ScheduleAndGrantLeases() = 0;

  /// Attempt to cancel all queued leases that match the predicate.
  ///
  /// \param predicate: A function that returns true if a lease needs to be cancelled.
  /// \param failure_type: The reason for cancellation.
  /// \param scheduling_failure_message: The reason message for cancellation.
  /// \return True if any lease was successfully cancelled.
  virtual bool CancelLeases(
      std::function<bool(const std::shared_ptr<internal::Work> &)> predicate,
      rpc::RequestWorkerLeaseReply::SchedulingFailureType failure_type,
      const std::string &scheduling_failure_message) = 0;

  /// Similar to `CancelLeases`. The only difference is that this method does not send
  /// RequestWorkerLease replies for those cancelled leases.
  /// \return A list of cancelled leases.
  virtual std::vector<std::shared_ptr<internal::Work>> CancelLeasesWithoutReply(
      std::function<bool(const std::shared_ptr<internal::Work> &)> predicate) = 0;

  virtual const absl::flat_hash_map<SchedulingClass,
                                    std::deque<std::shared_ptr<internal::Work>>>
      &GetLeasesToGrant() const = 0;

  virtual const absl::flat_hash_map<SchedulingClass,
                                    absl::flat_hash_map<WorkerID, int64_t>>
      &GetBackLogTracker() const = 0;

  virtual void SetWorkerBacklog(SchedulingClass scheduling_class,
                                const WorkerID &worker_id,
                                int64_t backlog_size) = 0;

  virtual void ClearWorkerBacklog(const WorkerID &worker_id) = 0;

  virtual const RayLease *AnyPendingLeasesForResourceAcquisition(
      int *num_pending_actor_creation, int *num_pending_leases) const = 0;

  virtual void LeasesUnblocked(const std::vector<LeaseID> &ready_ids) = 0;

  virtual void CleanupLease(std::shared_ptr<WorkerInterface> worker, RayLease *lease) = 0;

  virtual void ReleaseWorkerResources(std::shared_ptr<WorkerInterface> worker) = 0;

  virtual bool ReleaseCpuResourcesFromBlockedWorker(
      std::shared_ptr<WorkerInterface> worker) = 0;

  virtual bool ReturnCpuResourcesToUnblockedWorker(
      std::shared_ptr<WorkerInterface> worker) = 0;

  virtual ResourceSet CalcNormalTaskResources() const = 0;

  virtual void RecordMetrics() const = 0;

  virtual void DebugStr(std::stringstream &buffer) const = 0;

  virtual size_t GetNumLeaseSpilled() const = 0;
  virtual size_t GetNumWaitingLeaseSpilled() const = 0;
  virtual size_t GetNumUnschedulableLeaseSpilled() const = 0;
  virtual bool IsLeaseQueued(const SchedulingClass &scheduling_class,
                             const LeaseID &lease_id) const = 0;

  /// Add a reply callback to the lease. We don't overwrite the existing reply callback
  /// since due to message reordering we may receive the retry before the initial request.
  ///
  /// \param scheduling_class: The scheduling class of the lease.
  /// \param lease_id: The lease id of the lease.
  /// \param send_reply_callback: The callback used for the reply.
  /// \param reply: The reply of the lease request.
  ///
  /// \return True if the reply callback is added successfully.
  virtual bool AddReplyCallback(const SchedulingClass &scheduling_class,
                                const LeaseID &lease_id,
                                rpc::SendReplyCallback send_reply_callback,
                                rpc::RequestWorkerLeaseReply *reply) = 0;
};

/// A noop local lease manager. It is a no-op class. We need this because there's no
/// "LocalLeaseManager" when the `ClusterLeaseManager` is used within GCS. In the long
/// term, we should make `ClusterLeaseManager` not aware of `LocalLeaseManager`.
class NoopLocalLeaseManager : public LocalLeaseManagerInterface {
 public:
  NoopLocalLeaseManager() = default;

  void QueueAndScheduleLease(std::shared_ptr<internal::Work> work) override {
    RAY_CHECK(false)
        << "This function should never be called by gcs' local lease manager.";
  }

  void ScheduleAndGrantLeases() override {}

  bool CancelLeases(
      std::function<bool(const std::shared_ptr<internal::Work> &)> predicate,
      rpc::RequestWorkerLeaseReply::SchedulingFailureType failure_type,
      const std::string &scheduling_failure_message) override {
    return false;
  }

  std::vector<std::shared_ptr<internal::Work>> CancelLeasesWithoutReply(
      std::function<bool(const std::shared_ptr<internal::Work> &)> predicate) override {
    return {};
  }

  const absl::flat_hash_map<SchedulingClass, std::deque<std::shared_ptr<internal::Work>>>
      &GetLeasesToGrant() const override {
    static const absl::flat_hash_map<SchedulingClass,
                                     std::deque<std::shared_ptr<internal::Work>>>
        leases_to_grant;
    return leases_to_grant;
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

  const RayLease *AnyPendingLeasesForResourceAcquisition(
      int *num_pending_actor_creation, int *num_pending_leases) const override {
    return nullptr;
  }

  void LeasesUnblocked(const std::vector<LeaseID> &ready_ids) override {}

  void CleanupLease(std::shared_ptr<WorkerInterface> worker, RayLease *lease) override {}

  void ReleaseWorkerResources(std::shared_ptr<WorkerInterface> worker) override {}

  bool ReleaseCpuResourcesFromBlockedWorker(
      std::shared_ptr<WorkerInterface> worker) override {
    return false;
  }

  bool ReturnCpuResourcesToUnblockedWorker(
      std::shared_ptr<WorkerInterface> worker) override {
    return false;
  }

  ResourceSet CalcNormalTaskResources() const override { return ResourceSet(); }

  void RecordMetrics() const override{};

  void DebugStr(std::stringstream &buffer) const override {}

  size_t GetNumLeaseSpilled() const override { return 0; }
  size_t GetNumWaitingLeaseSpilled() const override { return 0; }
  size_t GetNumUnschedulableLeaseSpilled() const override { return 0; }
  bool IsLeaseQueued(const SchedulingClass &scheduling_class,
                     const LeaseID &lease_id) const override {
    return false;
  }
  bool AddReplyCallback(const SchedulingClass &scheduling_class,
                        const LeaseID &lease_id,
                        rpc::SendReplyCallback send_reply_callback,
                        rpc::RequestWorkerLeaseReply *reply) override {
    return false;
  }
};
}  // namespace raylet
}  // namespace ray
