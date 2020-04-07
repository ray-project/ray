#pragma once

#include <memory>
#include "absl/synchronization/mutex.h"
#include "invocation_spec.h"
#include "ray/core.h"

namespace ray {
namespace api {

class AbstractRayRuntime;

class ActorContext {
 public:
  std::shared_ptr<msgpack::sbuffer> current_actor = nullptr;

  std::shared_ptr<absl::Mutex> actor_mutex;

  ActorContext() { actor_mutex = std::shared_ptr<absl::Mutex>(new absl::Mutex); }
};

class TaskExecutor {
 public:
  /// TODO(Guyang Song): support multiple tasks execution
  std::unique_ptr<ObjectID> Execute(const InvocationSpec &invocation);

  static void Invoke(const TaskSpecification &task_spec,
                     std::shared_ptr<msgpack::sbuffer> actor,
                     AbstractRayRuntime *runtime);

  virtual ~TaskExecutor(){};
};
}  // namespace api
}  // namespace ray