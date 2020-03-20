
#pragma once

#include <ray/core.h>
#include <msgpack.hpp>

namespace ray {
namespace api {

class InvocationSpec {
 public:
  TaskID taskId;
  ActorID actorId;
  int actorCounter;
  long funcOffset;
  long execFuncOffset;
  std::shared_ptr<msgpack::sbuffer> args;
  int returnCount;
};
}  // namespace api
}  // namespace ray
