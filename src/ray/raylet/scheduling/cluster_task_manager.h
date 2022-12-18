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

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "ray/common/ray_object.h"
#include "ray/common/task/task.h"
#include "ray/common/task/task_common.h"
#include "ray/raylet/scheduling/cluster_resource_scheduler.h"
#include "ray/raylet/scheduling/cluster_task_manager_interface.h"
#include "ray/raylet/scheduling/internal.h"
#include "ray/raylet/scheduling/local_task_manager_interface.h"
#include "ray/raylet/scheduling/scheduler_resource_reporter.h"
#include "ray/raylet/scheduling/scheduler_stats.h"

namespace ray {
namespace raylet {

/// Schedules a task onto one node of the cluster. The logic is as follows:
/// 1. Queue tasks for scheduling.
/// 2. Pick a node on the cluster which has the available resources to run a
///    task.
///     * Step 2 should occur any time the state of the cluster is
///       changed, or a new task is queued.
/// 3. For tasks that's infeasable, put them into infeasible queue and reports
///    it to gcs, where the auto scaler will be notified and start new node
///    to accommodate the requirement.
class ClusterTaskManager : public ClusterTaskManagerInterface {
 public:
  /// \param self_node_id: ID of local node.
  /// \param cluster_resource_scheduler: The resource scheduler which contains
  ///                                    the state of the cluster.
  /// \param get_node_info: Function that returns the node info for a node.
  /// \param announce_infeasible_task: Callback that informs the user if a task
  ///                                  is infeasible.
  /// \param local_task_manager: Manages local tasks.
  /// \param get_time_ms: A callback which returns the current time in milliseconds.
  ClusterTaskManager(
      const NodeID &self_node_id,
      std::shared_ptr<ClusterResourceScheduler> cluster_resource_scheduler,
      internal::NodeInfoGetter get_node_info,
      std::function<void(const RayTask &)> announce_infeasible_task,
      std::shared_ptr<ILocalTaskManager> local_task_manager,
      std::function<int64_t(void)> get_time_ms = []() {
        return (int64_t)(absl::GetCurrentTimeNanos() / 1e6);
      });

  /// Queue task and schedule. This hanppens when processing the worker lease request.
  ///
  /// \param task: The incoming task to be queued and scheduled.
  /// \param grant_or_reject: True if we we should either grant or reject the request
  ///                         but no spillback.
  /// \param is_selected_based_on_locality : should schedule on local node if possible.
  /// \param reply: The reply of the lease request.
  /// \param send_reply_callback: The function used during dispatching.
  void QueueAndScheduleTask(const RayTask &task,
                            bool grant_or_reject,
                            bool is_selected_based_on_locality,
                            rpc::RequestWorkerLeaseReply *reply,
                            rpc::SendReplyCallback send_reply_callback) override;

  /// Attempt to cancel an already queued task.
  ///
  /// \param task_id: The id of the task to remove.
  /// \param failure_type: The failure type.
  ///
  /// \return True if task was successfully removed. This function will return
  /// false if the task is already running.
  bool CancelTask(const TaskID &task_id,
                  rpc::RequestWorkerLeaseReply::SchedulingFailureType failure_type =
                      rpc::RequestWorkerLeaseReply::SCHEDULING_CANCELLED_INTENDED,
                  const std::string &scheduling_failure_message = "") override;

  /// Cancel all tasks owned by a specific worker.
  bool CancelAllTaskOwnedBy(
      const WorkerID &worker_id,
      rpc::RequestWorkerLeaseReply::SchedulingFailureType failure_type =
          rpc::RequestWorkerLeaseReply::SCHEDULING_CANCELLED_INTENDED,
      const std::string &scheduling_failure_message = "") override;

  /// Populate the relevant parts of the heartbeat table. This is intended for
  /// sending resource usage of raylet to gcs. In particular, this should fill in
  /// resource_load and resource_load_by_shape.
  ///
  /// \param[out] data: Output parameter. `resource_load` and `resource_load_by_shape` are
  /// the only
  ///                   fields used.
  /// \param[in] last_reported_resources: The last reported resources. Used to check
  /// whether
  ///                                     resources have been changed.
  void FillResourceUsage(
      rpc::ResourcesData &data,
      const std::shared_ptr<NodeResources> &last_reported_resources = nullptr) override;

  /// Return if any tasks are pending resource acquisition.
  ///
  /// \param[out] example: An example task that is deadlocking.
  /// \param[in,out] any_pending: True if there's any pending example.
  /// \param[in,out] num_pending_actor_creation: Number of pending actor creation tasks.
  /// \param[in,out] num_pending_tasks: Number of pending tasks.
  /// \return True if any progress is any tasks are pending.
  bool AnyPendingTasksForResourceAcquisition(RayTask *example,
                                             bool *any_pending,
                                             int *num_pending_actor_creation,
                                             int *num_pending_tasks) const override;

  // Schedule and dispatch tasks.
  void ScheduleAndDispatchTasks() override;

  /// Record the internal metrics.
  void RecordMetrics() const override;

  /// The helper to dump the debug state of the cluster task manater.
  std::string DebugStr() const override;

  std::shared_ptr<ClusterResourceScheduler> GetClusterResourceScheduler() const;

  /// Get the count of tasks in `infeasible_tasks_`.
  size_t GetInfeasibleQueueSize() const;
  /// Get the count of tasks in `tasks_to_schedule_`.
  size_t GetPendingQueueSize() const;

  /// Populate the info of pending and infeasible actors. This function
  /// is only called by gcs node.
  ///
  /// \param[out] data: Output parameter. `resource_load_by_shape` is the only field
  /// filled.
  void FillPendingActorInfo(rpc::ResourcesData &data) const;

 private:
  void TryScheduleInfeasibleTask();

  // Schedule the task onto a node (which could be either remote or local).
  void ScheduleOnNode(const NodeID &node_to_schedule,
                      const std::shared_ptr<internal::Work> &work);

  /// Recompute the debug stats.
  /// It is needed because updating the debug state is expensive for cluster_task_manager.
  /// TODO(sang): Update the internal states value dynamically instead of iterating the
  /// data structure.
  void RecomputeDebugStats() const;

  const NodeID &self_node_id_;
  /// Responsible for resource tracking/view of the cluster.
  std::shared_ptr<ClusterResourceScheduler> cluster_resource_scheduler_;

  /// Function to get the node information of a given node id.
  internal::NodeInfoGetter get_node_info_;
  /// Function to announce infeasible task to GCS.
  std::function<void(const RayTask &)> announce_infeasible_task_;

  std::shared_ptr<ILocalTaskManager> local_task_manager_;

  /// TODO(swang): Add index from TaskID -> Work to avoid having to iterate
  /// through queues to cancel tasks, etc.
  /// Queue of lease requests that are waiting for resources to become available.
  /// Tasks move from scheduled -> dispatch | waiting.
  absl::flat_hash_map<SchedulingClass, std::deque<std::shared_ptr<internal::Work>>>
      tasks_to_schedule_;

  /// Queue of lease requests that are infeasible.
  /// Tasks go between scheduling <-> infeasible.
  absl::flat_hash_map<SchedulingClass, std::deque<std::shared_ptr<internal::Work>>>
      infeasible_tasks_;

  const SchedulerResourceReporter scheduler_resource_reporter_;
  mutable SchedulerStats internal_stats_;

  /// Returns the current time in milliseconds.
  std::function<int64_t()> get_time_ms_;

  friend class SchedulerStats;
  friend class ClusterTaskManagerTest;
  FRIEND_TEST(ClusterTaskManagerTest, FeasibleToNonFeasible);
};
}  // namespace raylet
}  // namespace ray
