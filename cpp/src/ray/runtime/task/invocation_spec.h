
#pragma once

#include <msgpack.hpp>
#include <ray/core.h>

namespace ray { namespace api {

class InvocationSpec {
 public:
  TaskID taskId;
  ActorID actorId;
  int actorCounter;
  int32_t func_offset;
  int32_t exec_func_offset;
  std::shared_ptr<msgpack::sbuffer> args;
  int returnCount;
};
}  }// namespace ray::api