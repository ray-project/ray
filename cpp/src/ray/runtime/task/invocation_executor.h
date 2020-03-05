#pragma once

#include "task_spec.h"

namespace ray {

class InvocationExecutor {
 public:
  static void execute(const TaskSpec &taskSpec, std::shared_ptr<msgpack::sbuffer> actor);
};
}  // namespace ray