
#pragma once

#include <ray/api/ray_runtime.h>
#include <msgpack.hpp>

#include "ray/core.h"

namespace ray {
namespace api {

class InvocationSpec {
 public:
  TaskType task_type;
  TaskID task_id;
  std::string name;
  ActorID actor_id;
  int actor_counter;
  std::string lib_name;
  RemoteFunctionPtrHolder fptr;
  std::vector<std::unique_ptr<::ray::TaskArg>> args;
};
}  // namespace api
}  // namespace ray
