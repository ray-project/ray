#pragma once

#include "ray/common/scheduling/cluster_resource_scheduler.h"
#include "ray/common/task/task.h"
#include "ray/common/task/task_common.h"
/* #include "ray/raylet/worker_pool.h" */

class ClusterTaskManager {

 public:
  ClusterTaskManager(ClusterResourceScheduler &scheduler, std::Function<bool(const Task&), const WorkerPool &worker_pool);

  /// For the pending task at the head of tasks_to_schedule_, return a node
  /// in the system (local or remote) that has enough resources available to
  /// run the task, if any such node exist.
  /// Repeat the process as long as we can schedule a task.
  void NewSchedulerSchedulePendingTasks();

  /// Dispatch tasks to available workers.
  std::vector<std::Pair<Task, Worker>> DispatchScheduledTasksToWorkers();

  /// Queue tasks for scheduling.
  void QueueTask(const Task &task);

  void TasksUnblocked(const std::vector<TaskID> readyIds);

 private:

  std::Function<bool(const Task&) fulfills_dependencies_func_;

  const WorkerPool &worker_pool_;

  /// Queue of lease requests that are waiting for resources to become available.
  /// TODO this should be a queue for each SchedulingClass
  std::deque<std::pair<ScheduleFn, Task>> tasks_to_schedule_;
  /// Queue of lease requests that should be scheduled onto workers.
  std::deque<std::pair<ScheduleFn, Task>> tasks_to_dispatch_;
  /// Queue tasks waiting for arguments to be transferred locally.
  absl::flat_hash_map<TaskID, std::pair<ScheduleFn, Task>> waiting_tasks_;
};
