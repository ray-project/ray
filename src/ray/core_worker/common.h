#ifndef RAY_CORE_WORKER_COMMON_H
#define RAY_CORE_WORKER_COMMON_H

#include <string>

#include "ray/common/buffer.h"
#include "ray/id.h"

namespace ray {

/// Type of this worker.
enum class WorkerType { WORKER, DRIVER };

/// Language of Ray tasks and workers.
enum class Language { PYTHON, JAVA };

/// Information about a remote function.
struct RayFunction {
  /// Language of the remote function.
  const Language language;
  /// Function descriptor of the remote function.
  const std::vector<std::string> function_descriptor;
};

/// Argument of a task.
struct TaskArg {
  /// Id of the argument, if passed by reference, otherwise nullptr.
  const std::shared_ptr<ObjectID> id;
  /// Data of the argument, if passed by value, otherwise nullptr.
  const std::shared_ptr<Buffer> data;
};

}  // namespace ray

#endif  // RAY_CORE_WORKER_COMMON_H
