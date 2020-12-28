#pragma once

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "ray/common/task/task.h"
#include "ray/common/task/task_common.h"
#include "ray/raylet/dependency_manager.h"
#include "ray/raylet/scheduling/cluster_resource_scheduler.h"
#include "ray/raylet/worker.h"
#include "ray/raylet/worker_pool.h"
#include "ray/rpc/grpc_client.h"
#include "ray/rpc/node_manager/node_manager_client.h"
#include "ray/rpc/node_manager/node_manager_server.h"

namespace ray {
namespace raylet {

/// Work represents all the information needed to make a scheduling decision.
/// This includes the task, the information we need to communicate to
/// dispatch/spillback and the callback to trigger it.
typedef std::tuple<Task, rpc::RequestWorkerLeaseReply *, std::function<void(void)>> Work;

typedef std::function<boost::optional<rpc::GcsNodeInfo>(const NodeID &node_id)>
    NodeInfoGetter;

/// Manages the queuing and dispatching of tasks. The logic is as follows:
/// 1. Queue tasks for scheduling.
/// 2. Pick a node on the cluster which has the available resources to run a
///    task.
///     * Step 2 should occur anytime any time the state of the cluster is
///       changed, or a new task is queued.
/// 3. If a task has unresolved dependencies, set it aside to wait for
///    dependencies to be resolved.
/// 4. When a task is ready to be dispatched, ensure that the local node is
///    still capable of running the task, then dispatch it.
///     * Step 4 should be run any time there is a new task to dispatch *or*
///       there is a new worker which can dispatch the tasks.
/// 5. When a worker finishes executing its task(s), the requester will return
///    it and we should release the resources in our view of the node's state.
class ClusterTaskManager {
 public:
  /// fullfills_dependencies_func Should return if all dependencies are
  /// fulfilled and unsubscribe from dependencies only if they're fulfilled. If
  /// a task has dependencies which are not fulfilled, wait for the
  /// dependencies to be fulfilled, then run on the local node.
  ///
  /// \param self_node_id: ID of local node.
  /// \param cluster_resource_scheduler: The resource scheduler which contains
  /// the state of the cluster.
  /// \param task_dependency_manager_ Used to fetch task's dependencies.
  /// \param is_owner_alive: A callback which returns if the owner process is alive
  /// (according to our ownership model).
  /// \param gcs_client: A gcs client.
  ClusterTaskManager(const NodeID &self_node_id,
                     std::shared_ptr<ClusterResourceScheduler> cluster_resource_scheduler,
                     TaskDependencyManagerInterface &task_dependency_manager,
                     std::function<bool(const WorkerID &, const NodeID &)> is_owner_alive,
                     NodeInfoGetter get_node_info,
                     std::function<void(const Task &)> announce_infeasible_task);

  /// (Step 2) For each task in tasks_to_schedule_, pick a node in the system
  /// (local or remote) that has enough resources available to run the task, if
  /// any such node exist. Skip tasks which are not schedulable.
  ///
  /// \return True if any tasks are ready for dispatch.
  bool SchedulePendingTasks();

  /// (Step 3) Attempts to dispatch all tasks which are ready to run. A task
  /// will be dispatched if it is on `tasks_to_dispatch_` and there are still
  /// available resources on the node.
  ///
  /// If there are not enough resources locally, up to one task per resource
  /// shape (the task at the head of the queue) will get spilled back to a
  /// different node.
  ///
  /// \param worker_pool: The pool of workers which will be dispatched to.
  /// `worker_pool` state will be modified (idle workers will be popped) during
  /// dispatching.
  void DispatchScheduledTasksToWorkers(
      WorkerPoolInterface &worker_pool,
      std::unordered_map<WorkerID, std::shared_ptr<WorkerInterface>> &leased_workers);

  /// (Step 1) Queue tasks for scheduling.
  /// \param fn: The function used during dispatching.
  /// \param task: The incoming task to schedule.
  void QueueTask(const Task &task, rpc::RequestWorkerLeaseReply *reply,
                 std::function<void(void)>);

  /// Move tasks from waiting to ready for dispatch. Called when a task's
  /// dependencies are resolved.
  ///
  /// \param readyIds: The tasks which are now ready to be dispatched.
  void TasksUnblocked(const std::vector<TaskID> ready_ids);

  /// (Step 5) Call once a task finishes (i.e. a worker is returned).
  ///
  /// \param worker: The worker which was running the task.
  void HandleTaskFinished(std::shared_ptr<WorkerInterface> worker);

  /// Attempt to cancel an already queued task.
  ///
  /// \param task_id: The id of the task to remove.
  ///
  /// \return True if task was successfully removed. This function will return
  /// false if the task is already running.
  bool CancelTask(const TaskID &task_id);

  /// Populate the list of pending or infeasible actor tasks for node stats.
  ///
  /// \param Output parameter.
  void FillPendingActorInfo(rpc::GetNodeStatsReply *reply) const;

  /// Populate the relevant parts of the heartbeat table. This is intended for
  /// sending raylet <-> gcs heartbeats. In particular, this should fill in
  /// resource_load and resource_load_by_shape.
  ///
  /// \param light_report_resource_usage_enabled Only send changed fields if true.
  /// \param Output parameter. `resource_load` and `resource_load_by_shape` are the only
  /// fields used.
  void FillResourceUsage(bool light_report_resource_usage_enabled,
                         std::shared_ptr<rpc::ResourcesData> data) const;

  /// Return if any tasks are pending resource acquisition.
  ///
  /// \param[in] exemplar An example task that is deadlocking.
  /// \param[in] num_pending_actor_creation Number of pending actor creation tasks.
  /// \param[in] num_pending_tasks Number of pending tasks.
  /// \param[in] any_pending True if there's any pending exemplar.
  /// \return True if any progress is any tasks are pending.
  bool AnyPendingTasks(Task *exemplar, bool *any_pending, int *num_pending_actor_creation,
                       int *num_pending_tasks) const;

  std::string DebugString() const;

 private:
  /// Helper method to try dispatching a single task from the queue to an
  /// available worker. Returns whether the task should be removed from the
  /// queue and whether the worker was successfully leased to execute the work.
  bool AttemptDispatchWork(const Work &work, std::shared_ptr<WorkerInterface> &worker,
                           bool *worker_leased);

  /// Reiterate all local infeasible tasks and register them to task_to_schedule_ if it
  /// becomes feasible to schedule.
  void TryLocalInfeasibleTaskScheduling();

  const NodeID &self_node_id_;
  std::shared_ptr<ClusterResourceScheduler> cluster_resource_scheduler_;
  /// Class to make task dependencies to be local.
  TaskDependencyManagerInterface &task_dependency_manager_;
  /// Function to check if the owner is alive on a given node.
  std::function<bool(const WorkerID &, const NodeID &)> is_owner_alive_;
  /// Function to get the node information of a given node id.
  NodeInfoGetter get_node_info_;
  /// Function to announce infeasible task to GCS.
  std::function<void(const Task &)> announce_infeasible_task_;

  const int max_resource_shapes_per_load_report_;
  const bool report_worker_backlog_;

  /// Queue of lease requests that are waiting for resources to become available.
  /// Tasks move from scheduled -> dispatch | waiting.
  std::unordered_map<SchedulingClass, std::deque<Work>> tasks_to_schedule_;

  /// Queue of lease requests that should be scheduled onto workers.
  /// Tasks move from scheduled | waiting -> dispatch.
  /// Tasks can also move from dispatch -> waiting if one of their arguments is
  /// evicted.
  /// All tasks in this map that have dependencies should be registered with
  /// the dependency manager, in case a dependency gets evicted while the task
  /// is still queued.
  std::unordered_map<SchedulingClass, std::deque<Work>> tasks_to_dispatch_;

  /// Tasks waiting for arguments to be transferred locally.
  /// Tasks move from waiting -> dispatch.
  /// Tasks can also move from dispatch -> waiting if one of their arguments is
  /// evicted.
  /// All tasks in this map that have dependencies should be registered with
  /// the dependency manager, so that they can be moved to dispatch once their
  /// dependencies are local.
  absl::flat_hash_map<TaskID, Work> waiting_tasks_;

  /// Queue of lease requests that are infeasible.
  /// Tasks go between scheduling <-> infeasible.
  std::unordered_map<SchedulingClass, std::deque<Work>> infeasible_tasks_;

  /// Track the cumulative backlog of all workers requesting a lease to this raylet.
  std::unordered_map<SchedulingClass, int> backlog_tracker_;

  /// Determine whether a task should be immediately dispatched,
  /// or placed on a wait queue.
  ///
  /// \return True if the work can be immediately dispatched.
  bool WaitForTaskArgsRequests(Work work);

  void Dispatch(
      std::shared_ptr<WorkerInterface> worker,
      std::unordered_map<WorkerID, std::shared_ptr<WorkerInterface>> &leased_workers_,
      const Task &task, rpc::RequestWorkerLeaseReply *reply,
      std::function<void(void)> send_reply_callback);

  void Spillback(const NodeID &spillback_to, const Work &work);

  void AddToBacklogTracker(const Task &task);
  void RemoveFromBacklogTracker(const Task &task);

  friend class ClusterTaskManagerTest;
};
}  // namespace raylet
}  // namespace ray
