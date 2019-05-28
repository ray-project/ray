#ifndef RAY_CORE_WORKER_TASK_EXECUTION_H
#define RAY_CORE_WORKER_TASK_EXECUTION_H

#include "common.h"
#include "ray/common/buffer.h"

namespace ray {

class CoreWorkerTaskExecutionInterface {
 public:
  CoreWorkerTaskExecutionInterface(std::shared_ptr<CoreWorker> core_worker)
      : core_worker_(core_worker) {}

  /// The callback provided app-language workers that executes tasks.
  ///
  /// \param ray_function[in] Information about the function to execute.
  /// \param args[in] Arguments of the task.
  /// \return Status.
  using TaskExecutor =
      std::function<Status(const RayFunction &ray_function, const std::vector<Buffer> &args)>;

  /// Start receving and executes tasks in a infinite loop.
  void StartWorker(const TaskExecutor &executor);

 private:
  /// Build arguments for task executor. This would loop through all the arguments
  /// in task spec, and for each of them that's passed by reference (ObjectID),
  /// fetch its content from store and; for arguments that are passedby value,
  /// just copy their content. 
  /// 
  /// \param spec[in] Task specification.
  /// \param args[out] The arguments for passing to task executor. 
  /// 
  Status BuildArgsForExecutor(const TaskSpecification &spec, std::vector<Arg> *args);

  /// Pointer to the CoreWorker instance.
  const std::shared_ptr<CoreWorker> core_worker_;
};

}  // namespace ray

#endif  // RAY_CORE_WORKER_TASK_EXECUTION_H
