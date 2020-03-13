#pragma once

#include <memory>

#include <ray/api/ray_runtime.h>
#include "invocation_spec.h"

namespace ray { namespace api {

class TaskSubmitter {
 public:
  TaskSubmitter();

  virtual ObjectID submitTask(const InvocationSpec &invocation) = 0;

  virtual ActorID createActor(
      remote_function_ptr_holder &fptr, std::shared_ptr<msgpack::sbuffer> args) = 0;

  virtual ObjectID submitActorTask(const InvocationSpec &invocation) = 0;

  virtual ~TaskSubmitter(){};
};
}  }// namespace ray::api