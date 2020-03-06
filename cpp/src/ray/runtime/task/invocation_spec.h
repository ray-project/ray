
#pragma once

#include <ray/api/uniqueId.h>
#include <msgpack.hpp>

namespace ray {

class InvocationSpec {
 public:
  UniqueId taskId;
  UniqueId actorId;
  int actorCounter;
  int32_t func_offset;
  int32_t exec_func_offset;
  std::shared_ptr<msgpack::sbuffer> args;
  int returnCount;
};
}  // namespace ray