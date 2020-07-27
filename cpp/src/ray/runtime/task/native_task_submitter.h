#pragma once

#include <boost/asio/thread_pool.hpp>
#include <memory>
#include <queue>
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

  ActorID CreateActor(RemoteFunctionPtrHolder &fptr,
                      std::shared_ptr<msgpack::sbuffer> args);

  ObjectID SubmitActorTask(const InvocationSpec &invocation);

 private:
  NativeRayRuntime &native_ray_tuntime_;

  ObjectID Submit(const InvocationSpec &invocation, TaskType type);
};
}  // namespace api
}  // namespace ray