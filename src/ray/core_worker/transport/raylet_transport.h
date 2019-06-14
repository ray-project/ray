#ifndef RAY_CORE_WORKER_RAYLET_TRANSPORT_H
#define RAY_CORE_WORKER_RAYLET_TRANSPORT_H

#include <list>

#include "ray/core_worker/transport/transport.h"
#include "ray/raylet/raylet_client.h"

namespace ray {

/// In raylet task submitter and receiver, a task is submitted to raylet, and possibly
/// gets forwarded to another raylet on which node the task should be executed, and
/// then a worker on that node gets this task and starts executing it.

class CoreWorkerRayletTaskSubmitter : public CoreWorkerTaskSubmitter {
 public:
  CoreWorkerRayletTaskSubmitter(RayletClient &raylet_client);

  /// Submit a task for execution to raylet.
  ///
  /// \param[in] task The task spec to submit.
  /// \return Status.
  virtual Status SubmitTask(const TaskSpec &task) override;

 private:
  /// Raylet client.
  RayletClient &raylet_client_;
};

class CoreWorkerRayletTaskReceiver : public CoreWorkerTaskReceiver {
 public:
  CoreWorkerRayletTaskReceiver(RayletClient &raylet_client);

  // Get tasks for execution from raylet.
  virtual Status GetTasks(std::vector<TaskSpec> *tasks) override;

 private:
  /// Raylet client.
  RayletClient &raylet_client_;
};

}  // namespace ray

#endif  // RAY_CORE_WORKER_RAYLET_TRANSPORT_H
