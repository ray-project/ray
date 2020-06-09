#include "native_task_submitter.h"
#include <ray/api/ray_exception.h>
#include "../../util/address_helper.h"
#include "../abstract_ray_runtime.h"

namespace ray {
namespace api {

NativeTaskSubmitter::NativeTaskSubmitter(NativeRayRuntime &native_ray_tuntime_)
    : native_ray_tuntime_(native_ray_tuntime_) {}

ObjectID NativeTaskSubmitter::Submit(const InvocationSpec &invocation, TaskType type) {
  return ObjectID();
}

ObjectID NativeTaskSubmitter::SubmitTask(const InvocationSpec &invocation) {
  return Submit(invocation, TaskType::NORMAL_TASK);
}

ActorID NativeTaskSubmitter::CreateActor(RemoteFunctionPtrHolder &fptr,
                                         std::shared_ptr<msgpack::sbuffer> args) {
  return native_ray_tuntime_.GetNextActorID();
}

ObjectID NativeTaskSubmitter::SubmitActorTask(const InvocationSpec &invocation) {
  return Submit(invocation, TaskType::ACTOR_TASK);
}

}  // namespace api
}  // namespace ray
