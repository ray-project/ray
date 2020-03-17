#pragma once

#include <ray/core.h>
namespace ray {
namespace api {

class InvocationExecutor {
 public:
  static void Execute(const TaskSpecification &taskSpec,
                      std::shared_ptr<msgpack::sbuffer> actor);
};
}  // namespace api
}  // namespace ray