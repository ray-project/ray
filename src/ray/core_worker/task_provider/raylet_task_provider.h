#ifndef RAY_CORE_WORKER_RAYLET_TASK_PROVIDER_H
#define RAY_CORE_WORKER_RAYLET_TASK_PROVIDER_H

#include <list>

#include "ray/core_worker/task_provider/task_provider.h"
#include "ray/raylet/raylet_client.h"

namespace ray {

/// In raylet task submission and execution provider, a task is submitted to raylet,
/// and possibly gets forwarded to another raylet on which node the task should be
/// executed, and then a worker on that node gets this task and starts executing it.

class CoreWorkerRayletTaskSubmissionProvider : public CoreWorkerTaskSubmissionProvider {
 public:
  CoreWorkerRayletTaskSubmissionProvider(RayClient &ray_client);

  /// Submit a task for execution to raylet.
  ///
  /// \param[in] task The task spec to submit.
  /// \return Status.
  virtual Status SubmitTask(const TaskSpec &task) override;
 private:
  /// ray client.
  RayClient &ray_client_;
};

class CoreWorkerRayletTaskExecutionProvider : public CoreWorkerTaskExecutionProvider {
 public:
  CoreWorkerRayletTaskExecutionProvider(RayClient &ray_client);

  // Get tasks for execution from raylet.
  virtual Status GetTasks(std::vector<TaskSpec> *tasks) override;
 private:
  /// ray client.
  RayClient &ray_client_;  
};

}  // namespace ray

#endif  // RAY_CORE_WORKER_RAYLET_TASK_PROVIDER_H