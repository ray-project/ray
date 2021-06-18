#pragma once

#include "../native_ray_runtime.h"
#include "invocation_spec.h"
#include "task_submitter.h"

namespace ray {
namespace api {

class NativeTaskSubmitter : public TaskSubmitter {
 public:
  ObjectID SubmitTask(InvocationSpec &invocation, const CallOptions &call_options);

  ActorID CreateActor(InvocationSpec &invocation,
                      const ActorCreationOptions &create_options);

  ObjectID SubmitActorTask(InvocationSpec &invocation, const CallOptions &call_options);

 private:
  ObjectID Submit(InvocationSpec &invocation, const CallOptions &call_options);
};
}  // namespace api
}  // namespace ray