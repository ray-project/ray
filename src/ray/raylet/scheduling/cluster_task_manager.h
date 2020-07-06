#pragma once

#include "ray/common/task/task.h"
#include "ray/common/task/task_common.h"
#include "ray/raylet/scheduling/cluster_resource_scheduler.h"
#include "ray/raylet/worker.h"
#include "ray/raylet/worker_pool.h"

namespace ray {
namespace raylet {

typedef std::function<void(std::shared_ptr<Worker>, ClientID spillback_to,
                           std::string address, int port)>
    ScheduleFn;

typedef std::pair<ScheduleFn, const Task &> Work;

class ClusterTaskManager {
 public:
  /// fullfills_dependencies_func Should return if all dependencies are
  /// fulfilled and unsubscribe from dependencies only if they're
  /// fulfilled.
  ClusterTaskManager(const ClientID &self_node_id,
                     std::shared_ptr<ClusterResourceScheduler> cluster_resource_scheduler,
                     std::function<bool(const Task &)> fulfills_dependencies_func,
                     const WorkerPool &worker_pool,
                     std::shared_ptr<gcs::GcsClient> gcs_client);

  /// For the pending task at the head of tasks_to_schedule_, return a node
  /// in the system (local or remote) that has enough resources available to
  /// run the task, if any such node exist.
  /// Repeat the process as long as we can schedule a task.
  bool SchedulePendingTasks();

  void DispatchScheduledTasksToWorkers();

  /// Dispatch tasks to available workers.
  std::unique_ptr<std::vector<std::pair<const Work, std::shared_ptr<Worker>>>>
  GetDispatchableTasks();

  /// Queue tasks for scheduling.
  void QueueTask(ScheduleFn fn, const Task &task);

  /// Move tasks from waiting to ready for dispatch
  void TasksUnblocked(const std::vector<TaskID> readyIds);

 private:
  const ClientID &self_node_id_;
  std::shared_ptr<ClusterResourceScheduler> cluster_resource_scheduler_;
  std::function<bool(const Task &)> fulfills_dependencies_func_;
  const WorkerPool &worker_pool_;
  std::shared_ptr<gcs::GcsClient> gcs_client_;

  /// Queue of lease requests that are waiting for resources to become available.
  /// TODO this should be a queue for each SchedulingClass
  std::deque<const Work> tasks_to_schedule_;
  /// Queue of lease requests that should be scheduled onto workers.
  std::deque<const Work> tasks_to_dispatch_;
  /// Queue tasks waiting for arguments to be transferred locally.
  absl::flat_hash_map<TaskID, const Work> waiting_tasks_;

  /* /// Correctly determine whether a task should be immediately dispatched, */
  /* /// or placed on a wait queue. */
  bool WaitForTaskArgsRequests(const Work &work);
};
}  // namespace raylet
}  // namespace ray
