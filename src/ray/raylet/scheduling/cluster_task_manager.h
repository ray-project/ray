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

    class ClusterTaskManager {

    public:
      ClusterTaskManager(
                        ClusterResourceScheduler &cluster_resource_scheduler,
                        std::function<bool(const Task &)> fulfills_dependencies_func,
                        const WorkerPool &worker_pool
                        );

      /// For the pending task at the head of tasks_to_schedule_, return a node
      /// in the system (local or remote) that has enough resources available to
      /// run the task, if any such node exist.
      /// Repeat the process as long as we can schedule a task.
      void NewSchedulerSchedulePendingTasks();

      /// Dispatch tasks to available workers.
      std::unique_ptr<std::vector<std::pair<Task, Worker>>> DispatchScheduledTasksToWorkers();

      /// Queue tasks for scheduling.
      void QueueTask(const Task &task);

      /// Move tasks from waiting to ready for dispatch
      void TasksUnblocked(const std::vector<TaskID> readyIds);

    private:
      std::shared_ptr<ClusterResourceScheduler> cluster_resource_scheduler_;
      std::function<bool(const Task&)> fulfills_dependencies_func_;
      const WorkerPool &worker_pool_;

      /// Queue of lease requests that are waiting for resources to become available.
      /// TODO this should be a queue for each SchedulingClass
      std::deque<std::pair<ScheduleFn, Task>> tasks_to_schedule_;
      /// Queue of lease requests that should be scheduled onto workers.
      std::deque<std::pair<ScheduleFn, Task>> tasks_to_dispatch_;
      /// Queue tasks waiting for arguments to be transferred locally.
      absl::flat_hash_map<TaskID, std::pair<ScheduleFn, Task>> waiting_tasks_;
    };
  }
}
