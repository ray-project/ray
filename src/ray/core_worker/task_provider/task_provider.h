#ifndef RAY_CORE_WORKER_TASK_PROVIDER_H
#define RAY_CORE_WORKER_TASK_PROVIDER_H

#include <list>

#include "ray/common/buffer.h"
#include "ray/common/id.h"
#include "ray/common/status.h"
#include "ray/core_worker/common.h"
#include "ray/raylet/task_spec.h"

namespace ray {


class CoreWorkerTaskSubmissionProvider {
 public:
  CoreWorkerTaskSubmissionProvider() {}

  /// Submit a task for execution.
  ///
  /// \param[in] task The task spec to submit.
  /// \return Status.
  virtual Status SubmitTask(const TaskSpec &task) = 0;
};

class CoreWorkerTaskExecutionProvider {
 public:
  CoreWorkerTaskExecutionProvider() {}

  // Get tasks for execution.
  virtual Status GetTasks(std::vector<TaskSpec> *tasks) = 0;
};

}  // namespace ray

#endif  // RAY_CORE_WORKER_TASK_PROVIDER_H