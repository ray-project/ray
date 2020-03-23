#pragma once

#include <memory>
#include "absl/synchronization/mutex.h"
#include "invocation_spec.h"
#include "ray/core.h"

namespace ray {
namespace api {

class ActorContext {
 public:
  std::shared_ptr<msgpack::sbuffer> currentActor = nullptr;

  std::shared_ptr<absl::Mutex> actorMutex;

  ActorContext() { actorMutex = std::shared_ptr<absl::Mutex>(new absl::Mutex); }
};

class TaskExecutor {
 public:
  /// TODO(Guyang Song): support multiple tasks execution
  std::unique_ptr<ObjectID> Execute(const InvocationSpec &invocation);

  static void Invoke(const TaskSpecification &taskSpec,
                     std::shared_ptr<msgpack::sbuffer> actor);

  virtual ~TaskExecutor(){};
};
}  // namespace api
}  // namespace ray