#pragma once

#include "ray/common/task/task.h"
#include "ray/common/task/task_common.h"
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

typedef std::function<boost::optional<rpc::GcsNodeInfo>(const ClientID &node_id)>
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
///    still capable of running the task.
///     * Step 4 should be run any time there is a new task to dispatch *or*
///       there is a new worker which can dispatch the tasks.
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
  /// \param fulfills_dependencies_func: Returns true if all of a task's
  /// dependencies are fulfilled.
  /// \param gcs_client: A gcs client.
  ClusterTaskManager(const ClientID &self_node_id,
                     std::shared_ptr<ClusterResourceScheduler> cluster_resource_scheduler,
                     std::function<bool(const Task &)> fulfills_dependencies_func,
                     NodeInfoGetter get_node_info);

  /// (Step 2) For each task in tasks_to_schedule_, pick a node in the system
  /// (local or remote) that has enough resources available to run the task, if
  /// any such node exist. Skip tasks which are not schedulable.
  ///
  /// \return True if any tasks are ready for dispatch.
  bool SchedulePendingTasks();

  /// (Step 3) Attempts to dispatch all tasks which are ready to run. A task
  /// will be dispatched if it is on `tasks_to_dispatch_` and there are still
  /// avaialable resources on the node.
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

 private:
  const ClientID &self_node_id_;
  std::shared_ptr<ClusterResourceScheduler> cluster_resource_scheduler_;
  std::function<bool(const Task &)> fulfills_dependencies_func_;
  NodeInfoGetter get_node_info_;

  /// Queue of lease requests that are waiting for resources to become available.
  /// TODO this should be a queue for each SchedulingClass
  std::deque<Work> tasks_to_schedule_;
  /// Queue of lease requests that should be scheduled onto workers.
  std::deque<Work> tasks_to_dispatch_;
  /// Tasks waiting for arguments to be transferred locally.
  absl::flat_hash_map<TaskID, Work> waiting_tasks_;

  /// Determine whether a task should be immediately dispatched,
  /// or placed on a wait queue.
  ///
  /// \return True if the work can be immediately dispatched.
  bool WaitForTaskArgsRequests(Work work);

  void Dispatch(
      std::shared_ptr<WorkerInterface> worker,
      std::unordered_map<WorkerID, std::shared_ptr<WorkerInterface>> &leased_workers_,
      const TaskSpecification &task_spec, rpc::RequestWorkerLeaseReply *reply,
      std::function<void(void)> send_reply_callback);

  void Spillback(ClientID spillback_to, std::string address, int port,
                 rpc::RequestWorkerLeaseReply *reply,
                 std::function<void(void)> send_reply_callback);
};
}  // namespace raylet
}  // namespace ray
