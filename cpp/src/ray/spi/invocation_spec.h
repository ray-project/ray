
#pragma once

#include <list>

#include <ray/api/blob.h>
#include <ray/api/uniqueId.h>

namespace ray {

class InvocationSpec {
 public:
  UniqueId taskId;
  UniqueId actorId;
  int actorCounter;
  // UniqueId                    functionId;
  int32_t func_offset;
  int32_t exec_func_offset;
  std::vector< ::ray::blob> args;
  int returnCount;
};
}  // namespace ray