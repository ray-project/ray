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
#include "ray/common/lease/lease.h"
#include "ray/raylet/scheduling/cluster_lease_manager_interface.h"
#include "ray/raylet/scheduling/cluster_resource_scheduler.h"
#include "ray/raylet/scheduling/local_lease_manager_interface.h"
#include "ray/raylet/scheduling/scheduler_resource_reporter.h"
#include "ray/raylet/scheduling/scheduler_stats.h"

namespace ray {
namespace raylet {

/// Schedules a lease onto one node of the cluster. The logic is as follows:
/// 1. Queue leases for scheduling.
/// 2. Pick a node on the cluster which has the available resources to run a
///    lease.
///     * Step 2 should occur any time the state of the cluster is
///       changed, or a new lease is queued.
/// 3. For leases that are infeasible, put them into infeasible queue and report
///    it to gcs, where the auto scaler will be notified and start a new node
///    to accommodate the requirement.
class ClusterLeaseManager : public ClusterLeaseManagerInterface {
 public:
  /// \param self_node_id: ID of local node.
  /// \param cluster_resource_scheduler: The resource scheduler which contains
  ///                                    the state of the cluster.
  /// \param get_node_info: Function that returns the node info for a node.
  /// \param announce_infeasible_lease: Callback that informs the user if a lease
  ///                                  is infeasible.
  /// \param local_lease_manager: Manages local leases.
  /// \param get_time_ms: A callback which returns the current time in milliseconds.
  ClusterLeaseManager(
      const NodeID &self_node_id,
      ClusterResourceScheduler &cluster_resource_scheduler,
      internal::NodeInfoGetter get_node_info,
      std::function<void(const RayLease &)> announce_infeasible_lease,
      LocalLeaseManagerInterface &local_lease_manager,
      std::function<int64_t(void)> get_time_ms = []() {
        return static_cast<int64_t>(absl::GetCurrentTimeNanos() / 1e6);
      });

  /// Queue lease and schedule. This happens when processing the worker lease request.
  ///
  /// \param lease: The incoming lease to be queued and scheduled.
  /// \param grant_or_reject: True if we we should either grant or reject the request
  ///                         but no spillback.
  /// \param is_selected_based_on_locality : should schedule on local node if possible.
  /// \param reply_callbacks: The reply callbacks of the lease request.
  void QueueAndScheduleLease(
      RayLease lease,
      bool grant_or_reject,
      bool is_selected_based_on_locality,
      std::vector<internal::ReplyCallback> reply_callbacks) override;

  /// Attempt to cancel an already queued lease.
  ///
  /// \param lease_id: The lease_id of the lease to remove.
  /// \param failure_type: The failure type.
  /// \param scheduling_failure_message: The failure message.
  ///
  /// \return True if lease was successfully cancelled. This function will return
  /// false if the lease is already granted.
  bool CancelLease(const LeaseID &lease_id,
                   rpc::RequestWorkerLeaseReply::SchedulingFailureType failure_type =
                       rpc::RequestWorkerLeaseReply::SCHEDULING_CANCELLED_INTENDED,
                   const std::string &scheduling_failure_message = "") override;

  bool CancelAllLeasesOwnedBy(
      const WorkerID &worker_id,
      rpc::RequestWorkerLeaseReply::SchedulingFailureType failure_type =
          rpc::RequestWorkerLeaseReply::SCHEDULING_CANCELLED_INTENDED,
      const std::string &scheduling_failure_message = "") override;

  bool CancelAllLeasesOwnedBy(
      const NodeID &node_id,
      rpc::RequestWorkerLeaseReply::SchedulingFailureType failure_type =
          rpc::RequestWorkerLeaseReply::SCHEDULING_CANCELLED_INTENDED,
      const std::string &scheduling_failure_message = "") override;

  /// Cancel all leases that requires certain resource shape.
  /// This function is intended to be used to cancel the infeasible leases. To make it a
  /// more general function, please modify the signature by adding parameters including
  /// the failure type and the failure message.
  ///
  /// \param target_resource_shapes: The resource shapes to cancel.
  ///
  /// \return True if any lease was successfully cancelled. This function will return
  /// false if the lease is already granted. This shouldn't happen in normal cases
  /// because the infeasible leases shouldn't be granted due to resource constraints.
  bool CancelLeasesWithResourceShapes(
      const std::vector<ResourceSet> target_resource_shapes) override;

  /// Attempt to cancel all queued leases that match the predicate.
  ///
  /// \param predicate: A function that returns true if a lease needs to be cancelled.
  /// \param failure_type: The reason for cancellation.
  /// \param scheduling_failure_message: The reason message for cancellation.
  /// \return True if any lease was successfully cancelled.
  bool CancelLeases(
      std::function<bool(const std::shared_ptr<internal::Work> &)> predicate,
      rpc::RequestWorkerLeaseReply::SchedulingFailureType failure_type,
      const std::string &scheduling_failure_message) override;

  /// Populate the relevant parts of the heartbeat table. This is intended for
  /// sending resource usage of raylet to gcs. In particular, this should fill in
  /// resource_load and resource_load_by_shape.
  ///
  /// \param[out] data: Output parameter. `resource_load` and `resource_load_by_shape` are
  /// the only fields used.
  void FillResourceUsage(rpc::ResourcesData &data) override;

  /// Return with an exemplar if any leases are pending resource acquisition.
  ///
  /// \param[in,out] num_pending_actor_creation: Number of pending actor creation leases.
  /// \param[in,out] num_pending_leases: Number of pending leases.
  /// \return An example lease that is deadlocking if any leases are pending resource
  /// acquisition.
  const RayLease *AnyPendingLeasesForResourceAcquisition(
      int *num_pending_actor_creation, int *num_pending_leases) const override;

  // Schedule and grant leases.
  void ScheduleAndGrantLeases() override;

  /// Record the internal metrics.
  void RecordMetrics() const override;

  /// The helper to dump the debug state of the cluster lease manater.
  std::string DebugStr() const override;

  ClusterResourceScheduler &GetClusterResourceScheduler() const;

  /// Get the count of leases in `infeasible_leases_`.
  size_t GetInfeasibleQueueSize() const;
  /// Get the count of leases in `leases_to_schedule_`.
  size_t GetPendingQueueSize() const;

  /// Populate the info of pending and infeasible actors. This function
  /// is only called by gcs node.
  ///
  /// \param[out] data: Output parameter. `resource_load_by_shape` is the only field
  /// filled.
  void FillPendingActorInfo(rpc::ResourcesData &data) const;

  /// Check if a lease is queued.
  ///
  /// \param scheduling_class: The scheduling class of the lease.
  /// \param lease_id: The lease id of the lease.
  ///
  /// \return True if the lease is queued in leases_to_schedule_ or infeasible_leases_.
  bool IsLeaseQueued(const SchedulingClass &scheduling_class,
                     const LeaseID &lease_id) const override;

  bool AddReplyCallback(const SchedulingClass &scheduling_class,
                        const LeaseID &lease_id,
                        rpc::SendReplyCallback send_reply_callback,
                        rpc::RequestWorkerLeaseReply *reply) override;

 private:
  void TryScheduleInfeasibleLease();

  // Schedule the lease onto a node (which could be to a worker thats in a local or remote
  // node).
  void ScheduleOnNode(const NodeID &node_to_schedule,
                      const std::shared_ptr<internal::Work> &work);

  /// Recompute the debug stats.
  /// It is needed because updating the debug state is expensive for
  /// cluster_lease_manager.
  /// TODO(sang): Update the internal states value dynamically instead of iterating the
  /// data structure.
  void RecomputeDebugStats() const;

  /// Whether the given Work matches the provided resource shape. The function checks
  /// the scheduling class of the work and compares it with each of the target resource
  /// shapes. If any of the resource shapes matches the resources of the scheduling
  /// class, the function returns true.
  ///
  /// \param work: The work to check.
  /// \param target_resource_shapes: The list of resource shapes to check against.
  ///
  /// \return True if the work matches any of the target resource shapes.
  bool IsWorkWithResourceShape(const std::shared_ptr<internal::Work> &work,
                               const std::vector<ResourceSet> &target_resource_shapes);

  const NodeID &self_node_id_;
  /// Responsible for resource tracking/view of the cluster.
  ClusterResourceScheduler &cluster_resource_scheduler_;

  /// Function to get the node information of a given node id.
  internal::NodeInfoGetter get_node_info_;
  /// Function to announce infeasible lease to GCS.
  std::function<void(const RayLease &)> announce_infeasible_lease_;

  LocalLeaseManagerInterface &local_lease_manager_;

  /// Queue of lease requests that are waiting for resources to become available.
  /// Leases move from scheduled -> dispatch | waiting.
  absl::flat_hash_map<SchedulingClass, std::deque<std::shared_ptr<internal::Work>>>
      leases_to_schedule_;

  /// Queue of lease requests that are infeasible.
  /// Leases go between scheduling <-> infeasible.
  absl::flat_hash_map<SchedulingClass, std::deque<std::shared_ptr<internal::Work>>>
      infeasible_leases_;

  const SchedulerResourceReporter scheduler_resource_reporter_;
  mutable SchedulerStats internal_stats_;

  /// Returns the current time in milliseconds.
  std::function<int64_t()> get_time_ms_;

  friend class SchedulerStats;
  friend class ClusterLeaseManagerTest;
  FRIEND_TEST(ClusterLeaseManagerTest, FeasibleToNonFeasible);
};
}  // namespace raylet
}  // namespace ray
