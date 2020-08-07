
#pragma once

#include <msgpack.hpp>
#include <ray/api/ray_runtime.h>
#include "ray/core.h"

namespace ray {
namespace api {

class InvocationSpec {
 public:
  TaskType task_type;
  TaskID task_id;
  ActorID actor_id;
  int actor_counter;
  // /// Remote function offset from base address.
  // size_t func_offset;
  // /// Executable function offset from base address.
  // size_t exec_func_offset;
  std::string lib_name;
  RemoteFunctionPtrHolder fptr;
  std::shared_ptr<msgpack::sbuffer> args;
};
}  // namespace api
}  // namespace ray
