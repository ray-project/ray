#pragma once

#include "../native_ray_runtime.h"
#include "invocation_spec.h"
#include "ray/core.h"
#include "task_submitter.h"

namespace ray {
namespace api {

class NativeTaskSubmitter : public TaskSubmitter {
 public:
  NativeTaskSubmitter(NativeRayRuntime &native_ray_tuntime);

  ObjectID SubmitTask(const InvocationSpec &invocation);

  ActorID CreateActor(const InvocationSpec &invocation);

  ObjectID SubmitActorTask(const InvocationSpec &invocation);

 private:
  NativeRayRuntime &native_ray_tuntime_;

  ObjectID Submit(const InvocationSpec &invocation);
};
}  // namespace api
}  // namespace ray