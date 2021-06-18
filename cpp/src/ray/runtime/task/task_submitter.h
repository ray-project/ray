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

  virtual ObjectID SubmitTask(InvocationSpec &invocation,
                              const CallOptions &call_options) = 0;

  virtual ActorID CreateActor(InvocationSpec &invocation,
                              const ActorCreationOptions &create_options) = 0;

  virtual ObjectID SubmitActorTask(InvocationSpec &invocation,
                                   const CallOptions &call_options) = 0;
};
}  // namespace api
}  // namespace ray