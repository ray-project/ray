#pragma once

#include <ray/api/ray_runtime.h>

#include <memory>

#include "invocation_spec.h"

namespace ray {
namespace api {

class TaskSubmitter {
 public:
  TaskSubmitter(){};

  virtual ~TaskSubmitter(){};

  virtual ObjectID SubmitTask(InvocationSpec &invocation) = 0;

  virtual ActorID CreateActor(InvocationSpec &invocation) = 0;

  virtual ObjectID SubmitActorTask(InvocationSpec &invocation) = 0;
};
}  // namespace api
}  // namespace ray