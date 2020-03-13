#pragma once

#include "task_spec.h"

namespace ray { namespace api {

class InvocationExecutor {
 public:
  static void execute(const LocalTaskSpec &taskSpec, std::shared_ptr<msgpack::sbuffer> actor);
};
}  }// namespace ray::api