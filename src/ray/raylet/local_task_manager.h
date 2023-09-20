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
#include "ray/raylet/dependency_manager.h"
#include "ray/raylet/scheduling/cluster_resource_scheduler.h"
#include "ray/raylet/scheduling/cluster_task_manager_interface.h"
#include "ray/raylet/scheduling/internal.h"
#include "ray/raylet/scheduling/local_task_manager_interface.h"
#include "ray/raylet/worker.h"
#include "ray/raylet/worker_pool.h"
#include "ray/rpc/grpc_client.h"
#include "ray/rpc/node_manager/node_manager_client.h"
#include "ray/rpc/node_manager/node_manager_server.h"

namespace ray {
namespace raylet {

/// Manages the lifetime of a task on the local node. It receives request from
/// cluster_task_manager (the distributed scheduler) and does the following
/// steps:
/// 1. Pulling task dependencies, add the task into waiting queue.
/// 2. Once task's dependencies are all pulled locally, the task be added into
///    dispatch queue.
/// 3. For all tasks in dispatch queue, we schedule them by first acquiring
///    local resources (including pinning the objects in memory and deduct
///    cpu/gpu and other resources from local reosource manager)) .
///    If a task failed to acquire resources in step 3, we will try to
///    spill it to an different remote node.
/// 4. If all resources are acquired, we start a worker and returns the worker
///    address to the client once worker starts up.
/// 5. When a worker finishes executing its task(s), the requester will return
///    it and we should release the resources in our view of the node's state.
/// 6. If a task has been waiting for arguments for too long, it will also be
///    spilled back to a different node.
///
/// TODO(scv119): ideally, the local scheduler shouldn't be responsible for spilling,
/// as it should return the request to the distributed scheduler if
/// resource accusition failed, or a task has arguments pending resolution for too long
/// time.
class LocalTaskManager : public ILocalTaskManager {
 public:
  /// \param self_node_id: ID of local node.
  /// \param cluster_resource_scheduler: The resource scheduler which contains
  ///                                    the state of the cluster.
  /// \param task_dependency_manager_ Used to fetch task's dependencies.
  /// \param is_owner_alive: A callback which returns if the owner process is alive
  ///                        (according to our ownership model).
  /// \param get_node_info: Function that returns the node info for a node.
  /// \param worker_pool: A reference to the worker pool.
  /// \param leased_workers: A reference to the leased workers map.
  /// \param get_task_arguments: A callback for getting a tasks' arguments by
  ///                            their ids.
  /// \param max_pinned_task_arguments_bytes: The cap on pinned arguments.
  /// \param get_time_ms: A callback which returns the current time in milliseconds.
  /// \param sched_cls_cap_interval_ms: The time before we increase the cap
  ///                                   on the number of tasks that can run per
  ///                                   scheduling class. If set to 0, there is no
  ///                                   cap. If it's a large number, the cap is hard.
  LocalTaskManager(
      const NodeID &self_node_id,
      std::shared_ptr<ClusterResourceScheduler> cluster_resource_scheduler,
      TaskDependencyManagerInterface &task_dependency_manager,
      std::function<bool(const WorkerID &, const NodeID &)> is_owner_alive,
      internal::NodeInfoGetter get_node_info,
      WorkerPoolInterface &worker_pool,
      absl::flat_hash_map<WorkerID, std::shared_ptr<WorkerInterface>> &leased_workers,
      std::function<bool(const std::vector<ObjectID> &object_ids,
                         std::vector<std::unique_ptr<RayObject>> *results)>
          get_task_arguments,
      size_t max_pinned_task_arguments_bytes,
      std::function<int64_t(void)> get_time_ms =
          []() { return (int64_t)(absl::GetCurrentTimeNanos() / 1e6); },
      int64_t sched_cls_cap_interval_ms =
          RayConfig::instance().worker_cap_initial_backoff_delay_ms());

  /// Queue task and schedule.
  void QueueAndScheduleTask(std::shared_ptr<internal::Work> work) override;

  // Schedule and dispatch tasks.
  void ScheduleAndDispatchTasks() override;

  /// Move tasks from waiting to ready for dispatch. Called when a task's
  /// dependencies are resolved.
  ///
  /// \param ready_ids: The tasks which are now ready to be dispatched.
  void TasksUnblocked(const std::vector<TaskID> &ready_ids);

  /// Return the finished task and release the worker resources.
  /// This method will be removed and can be replaced by `ReleaseWorkerResources` directly
  /// once we remove the legacy scheduler.
  ///
  /// \param worker: The worker which was running the task.
  /// \param task: Output parameter.
  void TaskFinished(std::shared_ptr<WorkerInterface> worker, RayTask *task);

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

  /// Call once a task finishes (i.e. a worker is returned).
  ///
  /// \param worker: The worker which was running the task.
  void ReleaseWorkerResources(std::shared_ptr<WorkerInterface> worker);

  /// When a task is blocked in ray.get or ray.wait, the worker who is executing the task
  /// should give up the CPU resources allocated for the running task for the time being
  /// and the worker itself should also be marked as blocked.
  ///
  /// \param worker: The worker who will give up the CPU resources.
  /// \return true if the cpu resources of the specified worker are released successfully,
  /// else false.
  bool ReleaseCpuResourcesFromUnblockedWorker(std::shared_ptr<WorkerInterface> worker);

  /// When a task is no longer blocked in a ray.get or ray.wait, the CPU resources that
  /// the worker gave up should be returned to it.
  ///
  /// \param worker The blocked worker.
  /// \return true if the cpu resources are returned back to the specified worker, else
  /// false.
  bool ReturnCpuResourcesToBlockedWorker(std::shared_ptr<WorkerInterface> worker);

  /// TODO(Chong-Li): Removing this and maintaining normal task resources by local
  /// resource manager.
  /// Calculate normal task resources.
  ResourceSet CalcNormalTaskResources() const;

  void SetWorkerBacklog(SchedulingClass scheduling_class,
                        const WorkerID &worker_id,
                        int64_t backlog_size);

  void ClearWorkerBacklog(const WorkerID &worker_id);

  const absl::flat_hash_map<SchedulingClass, std::deque<std::shared_ptr<internal::Work>>>
      &GetTaskToDispatch() const override {
    return tasks_to_dispatch_;
  }

  const absl::flat_hash_map<SchedulingClass, absl::flat_hash_map<WorkerID, int64_t>>
      &GetBackLogTracker() const override {
    return backlog_tracker_;
  }

  void RecordMetrics() const override;

  void DebugStr(std::stringstream &buffer) const override;

  size_t GetNumTaskSpilled() const override { return num_task_spilled_; }
  size_t GetNumWaitingTaskSpilled() const override { return num_waiting_task_spilled_; }
  size_t GetNumUnschedulableTaskSpilled() const override {
    return num_unschedulable_task_spilled_;
  }

 private:
  struct SchedulingClassInfo;

  void RemoveFromRunningTasksIfExists(const RayTask &task);

  /// Handle the popped worker from worker pool.
  bool PoppedWorkerHandler(const std::shared_ptr<WorkerInterface> worker,
                           PopWorkerStatus status,
                           const TaskID &task_id,
                           SchedulingClass scheduling_class,
                           const std::shared_ptr<internal::Work> &work,
                           bool is_detached_actor,
                           const rpc::Address &owner_address,
                           const std::string &runtime_env_setup_error_message);

  /// Attempts to dispatch all tasks which are ready to run. A task
  /// will be dispatched if it is on `tasks_to_dispatch_` and there are still
  /// available resources on the node.
  ///
  /// If there are not enough resources locally, up to one task per resource
  /// shape (the task at the head of the queue) will get spilled back to a
  /// different node.
  void DispatchScheduledTasksToWorkers();

  /// Helper method when the current node does not have the available resources to run a
  /// task.
  ///
  /// \returns true if the task was spilled. The task may not be spilled if the
  /// spillback policy specifies the local node (which may happen if no other nodes have
  /// the requested resources available).
  bool TrySpillback(const std::shared_ptr<internal::Work> &work, bool &is_infeasible);

  // Try to spill waiting tasks to a remote node, starting from the end of the
  // queue.
  void SpillWaitingTasks();

  /// Calculate the maximum number of running tasks for a given scheduling
  /// class. https://github.com/ray-project/ray/issues/16973
  ///
  /// \param sched_cls_id The scheduling class in question.
  /// \returns The maximum number instances of that scheduling class that
  ///          should be running (or blocked) at once.
  uint64_t MaxRunningTasksPerSchedulingClass(SchedulingClass sched_cls_id) const;

  /// Recompute the debug stats.
  /// It is needed because updating the debug state is expensive for cluster_task_manager.
  /// TODO(sang): Update the internal states value dynamically instead of iterating the
  /// data structure.
  void RecomputeDebugStats() const;

  /// Determine whether a task should be immediately dispatched,
  /// or placed on a wait queue.
  ///
  /// \return True if the work can be immediately dispatched.
  bool WaitForTaskArgsRequests(std::shared_ptr<internal::Work> work);

  void Dispatch(
      std::shared_ptr<WorkerInterface> worker,
      absl::flat_hash_map<WorkerID, std::shared_ptr<WorkerInterface>> &leased_workers_,
      const std::shared_ptr<TaskResourceInstances> &allocated_instances,
      const RayTask &task,
      rpc::RequestWorkerLeaseReply *reply,
      std::function<void(void)> send_reply_callback);

  void Spillback(const NodeID &spillback_to, const std::shared_ptr<internal::Work> &work);

  /// Sum up the backlog size across all workers for a given scheduling class.
  int64_t TotalBacklogSize(SchedulingClass scheduling_class);

  // Helper function to pin a task's args immediately before dispatch. This
  // returns false if there are missing args (due to eviction) or if there is
  // not enough memory available to dispatch the task, due to other executing
  // tasks' arguments.
  bool PinTaskArgsIfMemoryAvailable(const TaskSpecification &spec, bool *args_missing);

  // Helper functions to pin and release an executing task's args.
  void PinTaskArgs(const TaskSpecification &spec,
                   std::vector<std::unique_ptr<RayObject>> args);
  void ReleaseTaskArgs(const TaskID &task_id);

 private:
  const NodeID &self_node_id_;
  /// Responsible for resource tracking/view of the cluster.
  std::shared_ptr<ClusterResourceScheduler> cluster_resource_scheduler_;
  /// Class to make task dependencies to be local.
  TaskDependencyManagerInterface &task_dependency_manager_;
  /// Function to check if the owner is alive on a given node.
  std::function<bool(const WorkerID &, const NodeID &)> is_owner_alive_;
  /// Function to get the node information of a given node id.
  internal::NodeInfoGetter get_node_info_;

  const int max_resource_shapes_per_load_report_;

  /// Tracking information about the currently running tasks in a scheduling
  /// class. This information is used to place a cap on the number of running
  /// running tasks per scheduling class.
  struct SchedulingClassInfo {
    SchedulingClassInfo(int64_t cap)
        : running_tasks(),
          capacity(cap),
          next_update_time(std::numeric_limits<int64_t>::max()) {}
    /// Track the running task ids in this scheduling class.
    absl::flat_hash_set<TaskID> running_tasks;
    /// The total number of tasks that can run from this scheduling class.
    const uint64_t capacity;
    /// The next time that a new task of this scheduling class may be dispatched.
    int64_t next_update_time;
  };

  /// Mapping from scheduling class to information about the running tasks of
  /// the scheduling class. See `struct SchedulingClassInfo` above for more
  /// details about what information is tracked.
  absl::flat_hash_map<SchedulingClass, SchedulingClassInfo> info_by_sched_cls_;

  /// Queue of lease requests that should be scheduled onto workers.
  /// Tasks move from scheduled | waiting -> dispatch.
  /// Tasks can also move from dispatch -> waiting if one of their arguments is
  /// evicted.
  /// All tasks in this map that have dependencies should be registered with
  /// the dependency manager, in case a dependency gets evicted while the task
  /// is still queued.
  absl::flat_hash_map<SchedulingClass, std::deque<std::shared_ptr<internal::Work>>>
      tasks_to_dispatch_;

  /// Tasks waiting for arguments to be transferred locally.
  /// Tasks move from waiting -> dispatch.
  /// Tasks can also move from dispatch -> waiting if one of their arguments is
  /// evicted.
  /// All tasks in this map that have dependencies should be registered with
  /// the dependency manager, so that they can be moved to dispatch once their
  /// dependencies are local.
  ///
  /// We keep these in a queue so that tasks can be spilled back from the end
  /// of the queue. This is to try to prioritize spilling tasks whose
  /// dependencies may not be fetched locally yet.
  ///
  /// Note that because tasks can also move from dispatch -> waiting, the order
  /// in this queue may not match the order in which we initially received the
  /// tasks. This also means that the PullManager may request dependencies for
  /// these tasks in a different order than the waiting task queue.
  std::list<std::shared_ptr<internal::Work>> waiting_task_queue_;

  /// An index for the above queue.
  absl::flat_hash_map<TaskID, std::list<std::shared_ptr<internal::Work>>::iterator>
      waiting_tasks_index_;

  /// Track the backlog of all workers belonging to this raylet.
  absl::flat_hash_map<SchedulingClass, absl::flat_hash_map<WorkerID, int64_t>>
      backlog_tracker_;

  /// TODO(Shanly): Remove `worker_pool_` and `leased_workers_` and make them as
  /// parameters of methods if necessary once we remove the legacy scheduler.
  WorkerPoolInterface &worker_pool_;
  absl::flat_hash_map<WorkerID, std::shared_ptr<WorkerInterface>> &leased_workers_;

  /// Callback to get references to task arguments. These will be pinned while
  /// the task is running.
  std::function<bool(const std::vector<ObjectID> &object_ids,
                     std::vector<std::unique_ptr<RayObject>> *results)>
      get_task_arguments_;

  /// Arguments needed by currently granted lease requests. These should be
  /// pinned before the lease is granted to ensure that the arguments are not
  /// evicted before the task(s) start running.
  absl::flat_hash_map<TaskID, std::vector<ObjectID>> executing_task_args_;

  /// All arguments of running tasks, which are also pinned in the object
  /// store. The value is a pair: (the pointer to the object store that should
  /// be deleted once the object is no longer needed, number of tasks that
  /// depend on the object).
  absl::flat_hash_map<ObjectID, std::pair<std::unique_ptr<RayObject>, size_t>>
      pinned_task_arguments_;

  /// The total number of arguments pinned for running tasks.
  /// Used for debug purposes.
  size_t pinned_task_arguments_bytes_ = 0;

  /// The maximum amount of bytes that can be used by executing task arguments.
  size_t max_pinned_task_arguments_bytes_;

  /// Returns the current time in milliseconds.
  std::function<int64_t()> get_time_ms_;

  /// Whether or not to enable the worker process cap.
  const bool sched_cls_cap_enabled_;

  /// The initial interval before the cap on the number of worker processes is increased.
  const int64_t sched_cls_cap_interval_ms_;

  const int64_t sched_cls_cap_max_ms_;

  size_t num_task_spilled_ = 0;
  size_t num_waiting_task_spilled_ = 0;
  size_t num_unschedulable_task_spilled_ = 0;

  friend class SchedulerResourceReporter;
  friend class ClusterTaskManagerTest;
  friend class SchedulerStats;
  FRIEND_TEST(ClusterTaskManagerTest, FeasibleToNonFeasible);
};
}  // namespace raylet
}  // namespace ray
