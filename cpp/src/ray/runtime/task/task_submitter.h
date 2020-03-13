#pragma once

#include <memory>

#include <ray/api/ray_runtime.h>
#include "invocation_spec.h"

namespace ray { namespace api {

class TaskSubmitter {
 public:
  TaskSubmitter();

  virtual ObjectID SubmitTask(const InvocationSpec &invocation) = 0;

  virtual ActorID CreateActor(
      remote_function_ptr_holder &fptr, std::shared_ptr<msgpack::sbuffer> args) = 0;

  virtual ObjectID SubmitActorTask(const InvocationSpec &invocation) = 0;

  virtual ~TaskSubmitter(){};
};
}  }// namespace ray::api