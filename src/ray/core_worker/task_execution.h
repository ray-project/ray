#ifndef RAY_CORE_WORKER_TASK_EXECUTION_H
#define RAY_CORE_WORKER_TASK_EXECUTION_H

#include "common.h"
#include "ray/common/buffer.h"

namespace ray {

class CoreWorkerTaskExecutionInterface {
 public:
  CoreWorkerTaskExecutionInterface(CoreWorker *core_worker_)
      : core_worker_(core_worker) {}

  /// The callback provided app-language workers that executes tasks.
  ///
  /// \param ray_function[in] Information about the function to execute.
  /// \param args[in] Arguments of the task.
  /// \return Status.
  using TaskExecutor =
      std::function<Status(const RayFunction &ray_function, const vector<Buffer> &args)>;

  /// Start receving and executes tasks in a infinite loop.
  void StartWorker(const TaskExecutor &executor);

 private:
  const CoreWorker *core_worker_;
};

}  // namespace ray

#endif  // RAY_CORE_WORKER_TASK_EXECUTION_H
