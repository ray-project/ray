#pragma once

#include "../native_ray_runtime.h"
#include "invocation_spec.h"
#include "task_submitter.h"

namespace ray {
namespace internal {

class NativeTaskSubmitter : public TaskSubmitter {
 public:
  ObjectID SubmitTask(InvocationSpec &invocation);

  ActorID CreateActor(InvocationSpec &invocation);

  ObjectID SubmitActorTask(InvocationSpec &invocation);

 private:
  ObjectID Submit(InvocationSpec &invocation);
};
}  // namespace internal
}  // namespace ray