
#pragma once

#include <msgpack.hpp>
#include "ray/core.h"

namespace ray {
namespace api {

class InvocationSpec {
 public:
  TaskID task_id;
  ActorID actor_id;
  int actor_counter;
  /// Remote function offset from base address.
  size_t func_offset;
  /// Executable function offset from base address.
  size_t exec_func_offset;
  std::shared_ptr<msgpack::sbuffer> args;
};
}  // namespace api
}  // namespace ray
