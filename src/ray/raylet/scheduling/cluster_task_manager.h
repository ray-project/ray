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
#include "ray/raylet/worker.h"
#include "ray/raylet/worker_pool.h"
#include "ray/rpc/grpc_client.h"
#include "ray/rpc/node_manager/node_manager_client.h"
#include "ray/rpc/node_manager/node_manager_server.h"

namespace ray {
namespace raylet {

namespace internal {

enum class WorkStatus {
  /// Waiting to be scheduled.
  WAITING,
  /// Waiting for a worker to start.
  WAITING_FOR_WORKER,
  /// Queued task has been cancelled.
  CANCELLED,
};

/// This enum represents the cause of why work hasn't been scheduled yet.
enum class UnscheduledWorkCause {
  /// Waiting for acquiring resources.
  WAITING_FOR_RESOURCE_ACQUISITION,
  /// Waiting for more plasma store memory to be available. This is set when we can't pin
  /// task arguments due to the lack of memory.
  WAITING_FOR_AVAILABLE_PLASMA_MEMORY,
  /// Pending because there's no node that satisfies the resources in the cluster.
  WAITING_FOR_RESOURCES_AVAILABLE,
  /// Waiting because the worker wasn't available since job config for the worker wasn't
  /// registered yet.
  WORKER_NOT_FOUND_JOB_CONFIG_NOT_EXIST,
  /// Waiting becasue the worker wasn't available since its registration timed out.
  WORKER_NOT_FOUND_REGISTRATION_TIMEOUT,
  /// Waiting because the worker wasn't available since it was rate limited.
  WORKER_NOT_FOUND_RATE_LIMITED,
};

/// Work represents all the information needed to make a scheduling decision.
/// This includes the task, the information we need to communicate to
/// dispatch/spillback and the callback to trigger it.
class Work {
 public:
  RayTask task;
  const bool grant_or_reject;
  rpc::RequestWorkerLeaseReply *reply;
  std::function<void(void)> callback;
  std::shared_ptr<TaskResourceInstances> allocated_instances;
  Work(RayTask task, bool grant_or_reject, rpc::RequestWorkerLeaseReply *reply,
       std::function<void(void)> callback, WorkStatus status = WorkStatus::WAITING)
      : task(task),
        grant_or_reject(grant_or_reject),
        reply(reply),
        callback(callback),
        allocated_instances(nullptr),
        status_(status){};
  Work(const Work &Work) = delete;
  Work &operator=(const Work &work) = delete;
  ~Work() = default;

  /// Set the state as waiting with the cause.
  void SetStateWaiting(const UnscheduledWorkCause &cause) {
    status_ = WorkStatus::WAITING;
    unscheduled_work_cause_ = cause;
  }

  /// Set the state as waiting for workers, meaning it is waiting for workers to start.
  void SetStateWaitingForWorker() { status_ = WorkStatus::WAITING_FOR_WORKER; }

  /// Set the state as cancelled, meaning this task has to be unqueued from the node.
  void SetStateCancelled() { status_ = WorkStatus::CANCELLED; }

  WorkStatus GetState() const { return status_; }

  UnscheduledWorkCause GetUnscheduledCause() const { return unscheduled_work_cause_; }

 private:
  WorkStatus status_ = WorkStatus::WAITING;
  UnscheduledWorkCause unscheduled_work_cause_ =
      UnscheduledWorkCause::WAITING_FOR_RESOURCE_ACQUISITION;
};

typedef std::function<const rpc::GcsNodeInfo *(const NodeID &node_id)> NodeInfoGetter;

}  // namespace internal

/// Manages the queuing and dispatching of tasks. The logic is as follows:
/// 1. Queue tasks for scheduling.
/// 2. Pick a node on the cluster which has the available resources to run a
///    task.
///     * Step 2 should occur any time the state of the cluster is
///       changed, or a new task is queued.
/// 3. If a task has unresolved dependencies, set it aside to wait for
///    dependencies to be resolved.
/// 4. When a task is ready to be dispatched, ensure that the local node is
///    still capable of running the task, then dispatch it.
///     * Step 4 should be run any time there is a new task to dispatch *or*
///       there is a new worker which can dispatch the tasks.
/// 5. When a worker finishes executing its task(s), the requester will return
///    it and we should release the resources in our view of the node's state.
class ClusterTaskManager : public ClusterTaskManagerInterface {
 public:
  /// \param self_node_id: ID of local node.
  /// \param cluster_resource_scheduler: The resource scheduler which contains
  ///                                    the state of the cluster.
  /// \param task_dependency_manager_ Used to fetch task's dependencies.
  /// \param is_owner_alive: A callback which returns if the owner process is alive
  ///                        (according to our ownership model).
  /// \param get_node_info: Function that returns the node info for a node.
  /// \param announce_infeasible_task: Callback that informs the user if a task
  ///                                  is infeasible.
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
  ClusterTaskManager(
      const NodeID &self_node_id,
      std::shared_ptr<ClusterResourceScheduler> cluster_resource_scheduler,
      TaskDependencyManagerInterface &task_dependency_manager,
      std::function<bool(const WorkerID &, const NodeID &)> is_owner_alive,
      internal::NodeInfoGetter get_node_info,
      std::function<void(const RayTask &)> announce_infeasible_task,
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

  void SetWorkerBacklog(SchedulingClass scheduling_class, const WorkerID &worker_id,
                        int64_t backlog_size) override;

  void ClearWorkerBacklog(const WorkerID &worker_id) override;

  /// (Step 1) Queue tasks and schedule.
  /// Queue task and schedule. This hanppens when processing the worker lease request.
  ///
  /// \param task: The incoming task to be queued and scheduled.
  /// \param grant_or_reject: True if we we should either grant or reject the request
  ///                         but no spillback.
  /// \param reply: The reply of the lease request.
  /// \param send_reply_callback: The function used during dispatching.
  void QueueAndScheduleTask(const RayTask &task, bool grant_or_reject,
                            rpc::RequestWorkerLeaseReply *reply,
                            rpc::SendReplyCallback send_reply_callback) override;

  /// Move tasks from waiting to ready for dispatch. Called when a task's
  /// dependencies are resolved.
  ///
  /// \param readyIds: The tasks which are now ready to be dispatched.
  void TasksUnblocked(const std::vector<TaskID> &ready_ids) override;

  /// Return the finished task and release the worker resources.
  /// This method will be removed and can be replaced by `ReleaseWorkerResources` directly
  /// once we remove the legacy scheduler.
  ///
  /// \param worker: The worker which was running the task.
  /// \param task: Output parameter.
  void TaskFinished(std::shared_ptr<WorkerInterface> worker, RayTask *task) override;

  /// Attempt to cancel an already queued task.
  ///
  /// \param task_id: The id of the task to remove.
  /// \param runtime_env_setup_failed: If this is being cancelled because the env setup
  ///                                  failed.
  ///
  /// \return True if task was successfully removed. This function will return
  /// false if the task is already running.
  bool CancelTask(const TaskID &task_id, bool runtime_env_setup_failed = false) override;

  /// Populate the list of pending or infeasible actor tasks for node stats.
  ///
  /// \param[out] reply: Output parameter. `infeasible_tasks` is the only field filled.
  void FillPendingActorInfo(rpc::GetNodeStatsReply *reply) const override;

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
  void FillResourceUsage(rpc::ResourcesData &data,
                         const std::shared_ptr<SchedulingResources>
                             &last_reported_resources = nullptr) override;

  /// Return if any tasks are pending resource acquisition.
  ///
  /// \param[out] exemplar: An example task that is deadlocking.
  /// \param[in,out] num_pending_actor_creation: Number of pending actor creation tasks.
  /// \param[in,out] num_pending_tasks: Number of pending tasks.
  /// \param[in,out] any_pending: True if there's any pending exemplar.
  /// \return True if any progress is any tasks are pending.
  bool AnyPendingTasksForResourceAcquisition(RayTask *exemplar, bool *any_pending,
                                             int *num_pending_actor_creation,
                                             int *num_pending_tasks) const override;

  /// (Step 5) Call once a task finishes (i.e. a worker is returned).
  ///
  /// \param worker: The worker which was running the task.
  void ReleaseWorkerResources(std::shared_ptr<WorkerInterface> worker) override;

  /// When a task is blocked in ray.get or ray.wait, the worker who is executing the task
  /// should give up the CPU resources allocated for the running task for the time being
  /// and the worker itself should also be marked as blocked.
  ///
  /// \param worker: The worker who will give up the CPU resources.
  /// \return true if the cpu resources of the specified worker are released successfully,
  /// else false.
  bool ReleaseCpuResourcesFromUnblockedWorker(
      std::shared_ptr<WorkerInterface> worker) override;

  /// When a task is no longer blocked in a ray.get or ray.wait, the CPU resources that
  /// the worker gave up should be returned to it.
  ///
  /// \param worker The blocked worker.
  /// \return true if the cpu resources are returned back to the specified worker, else
  /// false.
  bool ReturnCpuResourcesToBlockedWorker(
      std::shared_ptr<WorkerInterface> worker) override;

  // Schedule and dispatch tasks.
  void ScheduleAndDispatchTasks() override;

  /// Record the internal metrics.
  void RecordMetrics() const override;

  /// The helper to dump the debug state of the cluster task manater.
  std::string DebugStr() const override;

  /// Check if there are enough available resources for the given input.
  bool IsLocallySchedulable(const RayTask &task) const override;

  /// Calculate normal task resources.
  ResourceSet CalcNormalTaskResources() const override;

 private:
  struct SchedulingClassInfo;

  /// (Step 2) For each task in tasks_to_schedule_, pick a node in the system
  /// (local or remote) that has enough resources available to run the task, if
  /// any such node exist. Skip tasks which are not schedulable.
  ///
  /// \return True if any tasks are ready for dispatch.
  bool SchedulePendingTasks();

  void RemoveFromRunningTasksIfExists(const RayTask &task);

  /// Handle the popped worker from worker pool.
  bool PoppedWorkerHandler(const std::shared_ptr<WorkerInterface> worker,
                           PopWorkerStatus status, const TaskID &task_id,
                           SchedulingClass scheduling_class,
                           const std::shared_ptr<internal::Work> &work,
                           bool is_detached_actor, const rpc::Address &owner_address);

  /// (Step 3) Attempts to dispatch all tasks which are ready to run. A task
  /// will be dispatched if it is on `tasks_to_dispatch_` and there are still
  /// available resources on the node.
  ///
  /// If there are not enough resources locally, up to one task per resource
  /// shape (the task at the head of the queue) will get spilled back to a
  /// different node.
  void DispatchScheduledTasksToWorkers(
      WorkerPoolInterface &worker_pool,
      absl::flat_hash_map<WorkerID, std::shared_ptr<WorkerInterface>> &leased_workers);

  /// Helper method when the current node does not have the available resources to run a
  /// task.
  ///
  /// \returns true if the task was spilled. The task may not be spilled if the
  /// spillback policy specifies the local node (which may happen if no other nodes have
  /// the requested resources available).
  bool TrySpillback(const std::shared_ptr<internal::Work> &work, bool &is_infeasible);

  /// Reiterate all local infeasible tasks and register them to task_to_schedule_ if it
  /// becomes feasible to schedule.
  void TryLocalInfeasibleTaskScheduling();

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

  /// Helper method to get the best node for running the task.
  std::string GetBestSchedulableNode(const internal::Work &work,
                                     bool requires_object_store_memory,
                                     bool force_spillback, bool *is_infeasible);

  /// Recompute the debug stats.
  /// It is needed because updating the debug state is expensive for cluster_task_manager.
  /// TODO(sang): Update the internal states value dynamically instead of iterating the
  /// data structure.
  void RecomputeDebugStats() const;

  const NodeID &self_node_id_;
  /// Responsible for resource tracking/view of the cluster.
  std::shared_ptr<ClusterResourceScheduler> cluster_resource_scheduler_;
  /// Class to make task dependencies to be local.
  TaskDependencyManagerInterface &task_dependency_manager_;
  /// Function to check if the owner is alive on a given node.
  std::function<bool(const WorkerID &, const NodeID &)> is_owner_alive_;
  /// Function to get the node information of a given node id.
  internal::NodeInfoGetter get_node_info_;
  /// Function to announce infeasible task to GCS.
  std::function<void(const RayTask &)> announce_infeasible_task_;

  const int max_resource_shapes_per_load_report_;

  /// TODO(swang): Add index from TaskID -> Work to avoid having to iterate
  /// through queues to cancel tasks, etc.
  /// Queue of lease requests that are waiting for resources to become available.
  /// Tasks move from scheduled -> dispatch | waiting.
  absl::flat_hash_map<SchedulingClass, std::deque<std::shared_ptr<internal::Work>>>
      tasks_to_schedule_;

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

  /// Queue of lease requests that are infeasible.
  /// Tasks go between scheduling <-> infeasible.
  absl::flat_hash_map<SchedulingClass, std::deque<std::shared_ptr<internal::Work>>>
      infeasible_tasks_;

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

  struct InternalStats {
    /// Number of tasks that are spilled to other
    /// nodes because it cannot be scheduled locally.
    int64_t metric_tasks_spilled = 0;
    /// Number of tasks that are waiting for
    /// resources to be available locally.
    int64_t num_waiting_for_resource = 0;
    /// Number of tasks that are waiting for available memory
    /// from the plasma store.
    int64_t num_waiting_for_plasma_memory = 0;
    /// Number of tasks that are waiting for nodes with available resources.
    int64_t num_waiting_for_remote_node_resources = 0;
    /// Number of workers that couldn't be started because the job config wasn't local.
    int64_t num_worker_not_started_by_job_config_not_exist = 0;
    /// Number of workers that couldn't be started because the worker registration timed
    /// out.
    int64_t num_worker_not_started_by_registration_timeout = 0;
    /// Number of workers that couldn't be started becasue it hits the worker startup rate
    /// limit.
    int64_t num_worker_not_started_by_process_rate_limit = 0;
    /// Number of tasks that are waiting for worker processes to start.
    int64_t num_tasks_waiting_for_workers = 0;
    /// Number of cancelled tasks.
    int64_t num_cancelled_tasks = 0;
    /// Number of infeasible tasks.
    int64_t num_infeasible_tasks = 0;
    /// Number of tasks to schedule.
    int64_t num_tasks_to_schedule = 0;
    /// Number of tasks to dispatch.
    int64_t num_tasks_to_dispatch = 0;
  };

  mutable InternalStats internal_stats_;

  /// Determine whether a task should be immediately dispatched,
  /// or placed on a wait queue.
  ///
  /// \return True if the work can be immediately dispatched.
  bool WaitForTaskArgsRequests(std::shared_ptr<internal::Work> work);

  void Dispatch(
      std::shared_ptr<WorkerInterface> worker,
      absl::flat_hash_map<WorkerID, std::shared_ptr<WorkerInterface>> &leased_workers_,
      const std::shared_ptr<TaskResourceInstances> &allocated_instances,
      const RayTask &task, rpc::RequestWorkerLeaseReply *reply,
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

  friend class ClusterTaskManagerTest;
  FRIEND_TEST(ClusterTaskManagerTest, FeasibleToNonFeasible);
};
}  // namespace raylet
}  // namespace ray
