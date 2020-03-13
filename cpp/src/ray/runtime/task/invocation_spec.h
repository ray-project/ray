
#pragma once

#include <msgpack.hpp>
#include <ray/core.h>

namespace ray { namespace api {

class InvocationSpec {
 public:
  TaskID taskId;
  ActorID actorId;
  int actorCounter;
  int32_t funcOffset;
  int32_t execFuncOffset;
  std::shared_ptr<msgpack::sbuffer> args;
  int returnCount;
};
}  }// namespace ray::api