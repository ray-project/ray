#ifndef RAY_CORE_WORKER_COMMON_H
#define RAY_CORE_WORKER_COMMON_H

#include <string>

#include "ray/id.h"
#include "ray/common/io.h"

namespace ray {

/// Type of this worker.
enum class WorkerType { WORKER, DRIVER };

/// Language of Ray tasks and workers.
enum class Language { PYTHON, JAVA };

/// Information about a remote function.
struct RayFunction {
  /// Language of the remote function.
  Language language;
  /// Function descriptor of the remote function.
  std::vector<std::string> function_descriptors;
};

/// Argument of a task.
struct Arg {
  /// Id of the argument, if passed by reference, otherwise nullptr.
  std::shared_ptr<ObjectID> id;
  /// Data of the argument, if passed by value, otherwise nullptr.
  std::shared_ptr<Buffer> *data;
};

}  // namespace ray

#endif  // RAY_CORE_WORKER_COMMON_H
