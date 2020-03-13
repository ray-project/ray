#pragma once

#include <memory>

#include <ray/core.h>
#include "invocation_spec.h"

namespace ray { namespace api {

class ActorContext {
 public:
  std::shared_ptr<msgpack::sbuffer> currentActor = nullptr;

  std::shared_ptr<std::mutex> actorMutex;

  ActorContext() {
    actorMutex = std::shared_ptr<std::mutex>(new std::mutex);
  }
};

class TaskExcuter {
 public:
  /// TODO: support multiple tasks execution
  std::unique_ptr<ObjectID> Execute(const InvocationSpec &invocation);

  virtual ~TaskExcuter(){};
};
}  }// namespace ray::api