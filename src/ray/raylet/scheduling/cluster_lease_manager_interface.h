// Copyright 2017 The Ray Authors.
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
#include <string>

#include "ray/raylet/scheduling/internal.h"
#include "ray/rpc/rpc_callback_types.h"
#include "src/ray/protobuf/node_manager.pb.h"

namespace ray {
namespace raylet {
class ClusterLeaseManagerInterface {
 public:
  virtual ~ClusterLeaseManagerInterface() = default;

  // Schedule and dispatch leases.
  virtual void ScheduleAndGrantLeases() = 0;

  /// Populate the relevant parts of the heartbeat table. This is intended for
  /// sending raylet <-> gcs heartbeats. In particular, this should fill in
  /// resource_load and resource_load_by_shape.
  ///
  /// \param Output parameter. `resource_load` and `resource_load_by_shape` are the only
  /// fields used.
  virtual void FillResourceUsage(rpc::ResourcesData &data) = 0;

  /// Attempt to cancel an already queued lease.
  ///
  /// \param lease_id: The id of the lease to remove.
  /// \param failure_type: The failure type.
  /// \param scheduling_failure_message: The failure message.
  ///
  /// \return True if lease was successfully cancelled. This function will return
  /// false if the lease is already granted.
  virtual bool CancelLease(
      const LeaseID &lease_id,
      rpc::RequestWorkerLeaseReply::SchedulingFailureType failure_type =
          rpc::RequestWorkerLeaseReply::SCHEDULING_CANCELLED_INTENDED,
      const std::string &scheduling_failure_message = "") = 0;

  /// Cancel all leases owned by a specific worker.
  virtual bool CancelAllLeasesOwnedBy(
      const WorkerID &worker_id,
      rpc::RequestWorkerLeaseReply::SchedulingFailureType failure_type =
          rpc::RequestWorkerLeaseReply::SCHEDULING_CANCELLED_INTENDED,
      const std::string &scheduling_failure_message = "") = 0;

  /// Cancel all leases owned by a worker on the specific node.
  virtual bool CancelAllLeasesOwnedBy(
      const NodeID &node_id,
      rpc::RequestWorkerLeaseReply::SchedulingFailureType failure_type =
          rpc::RequestWorkerLeaseReply::SCHEDULING_CANCELLED_INTENDED,
      const std::string &scheduling_failure_message = "") = 0;

  /// Attempt to cancel all queued leases that match the resource shapes.
  /// This function is intended to be used to cancel the infeasible leases. To make it a
  /// more general function, please modify the signature by adding parameters including
  /// the failure type and the failure message.
  ///
  /// \param target_resource_shapes: The resource shapes to cancel.
  ///
  /// \return True if any lease was successfully removed. This function will return false
  /// if the lease is already running. This shouldn't happen in noremal cases because the
  /// infeasible leases shouldn't be able to run due to resource constraints.
  virtual bool CancelLeasesWithResourceShapes(
      const std::vector<ResourceSet> target_resource_shapes) = 0;

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

  /// Queue lease and schedule. This happens when processing the worker lease request.
  ///
  /// \param lease: The incoming lease to be queued and scheduled.
  /// \param grant_or_reject: True if we we should either grant or reject the request
  ///                         but no spillback.
  /// \param reply: The reply of the lease request.
  /// \param send_reply_callback: The function used during dispatching.
  virtual void QueueAndScheduleLease(
      RayLease lease,
      bool grant_or_reject,
      bool is_selected_based_on_locality,
      std::vector<internal::ReplyCallback> reply_callbacks) = 0;

  /// Return with an exemplar if any leases are pending resource acquisition.
  ///
  /// \param[in] num_pending_actor_creation Number of pending actor creation tasks.
  /// \param[in] num_pending_leases Number of pending leases.
  /// \return An example lease that is deadlocking if any leases are pending resource
  /// acquisition.
  virtual const RayLease *AnyPendingLeasesForResourceAcquisition(
      int *num_pending_actor_creation, int *num_pending_leases) const = 0;

  /// The helper to dump the debug state of the cluster lease manater.
  virtual std::string DebugStr() const = 0;

  /// Record the internal metrics.
  virtual void RecordMetrics() const = 0;

  /// Check if a lease is queued.
  ///
  /// \param scheduling_class: The scheduling class of the lease.
  /// \param lease_id: The lease id of the lease.
  ///
  /// \return True if the lease is queued in leases_to_schedule_ or infeasible_leases_.
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
}  // namespace raylet
}  // namespace ray
