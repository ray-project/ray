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

#include "ray/rpc/server_call.h"
#include "src/ray/protobuf/node_manager.pb.h"

namespace ray {
namespace raylet {
class ClusterTaskManagerInterface {
 public:
  virtual ~ClusterTaskManagerInterface() = default;

  // Schedule and dispatch tasks.
  virtual void ScheduleAndDispatchTasks() = 0;
  ;
  /// Populate the relevant parts of the heartbeat table. This is intended for
  /// sending raylet <-> gcs heartbeats. In particular, this should fill in
  /// resource_load and resource_load_by_shape.
  ///
  /// \param Output parameter. `resource_load` and `resource_load_by_shape` are the only
  /// fields used.
  virtual void FillResourceUsage(
      rpc::ResourcesData &data,
      const std::shared_ptr<SchedulingResources> &last_reported_resources = nullptr) = 0;

  /// Populate the list of pending or infeasible actor tasks for node stats.
  ///
  /// \param Output parameter.
  virtual void FillPendingActorInfo(rpc::GetNodeStatsReply *reply) const = 0;

  /// Attempt to cancel an already queued task.
  ///
  /// \param task_id: The id of the task to remove.
  /// \param failure_type: The failure type.
  ///
  /// \return True if task was successfully removed. This function will return
  /// false if the task is already running.
  virtual bool CancelTask(
      const TaskID &task_id,
      rpc::RequestWorkerLeaseReply::SchedulingFailureType failure_type =
          rpc::RequestWorkerLeaseReply::SCHEDULING_CANCELLED_INTENDED,
      const std::string &scheduling_failure_message = "") = 0;

  /// Queue task and schedule. This hanppens when processing the worker lease request.
  ///
  /// \param task: The incoming task to be queued and scheduled.
  /// \param grant_or_reject: True if we we should either grant or reject the request
  ///                         but no spillback.
  /// \param reply: The reply of the lease request.
  /// \param send_reply_callback: The function used during dispatching.
  virtual void QueueAndScheduleTask(const RayTask &task,
                                    bool grant_or_reject,
                                    bool is_selected_based_on_locality,
                                    rpc::RequestWorkerLeaseReply *reply,
                                    rpc::SendReplyCallback send_reply_callback) = 0;

  /// Return if any tasks are pending resource acquisition.
  ///
  /// \param[in] exemplar An example task that is deadlocking.
  /// \param[in] num_pending_actor_creation Number of pending actor creation tasks.
  /// \param[in] num_pending_tasks Number of pending tasks.
  /// \param[in] any_pending True if there's any pending exemplar.
  /// \return True if any progress is any tasks are pending.
  virtual bool AnyPendingTasksForResourceAcquisition(RayTask *exemplar,
                                                     bool *any_pending,
                                                     int *num_pending_actor_creation,
                                                     int *num_pending_tasks) const = 0;

  /// The helper to dump the debug state of the cluster task manater.
  virtual std::string DebugStr() const = 0;

  /// Record the internal metrics.
  virtual void RecordMetrics() const = 0;
};
}  // namespace raylet
}  // namespace ray
