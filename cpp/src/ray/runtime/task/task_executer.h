#pragma once

#include <memory>

#include "invocation_spec.h"

namespace ray {

class ActorContext {
  public:
    std::unique_ptr<::ray::blob> currentActor = NULL;

    ActorContext() {
    }
 };

class TaskExcuter {

 public:

  /// TODO: support multiple tasks execution
  std::unique_ptr<UniqueId> execute(const InvocationSpec &invocation);

  virtual void maybeSaveCheckpoint() = 0;

  virtual void maybeLoadCheckpoint() = 0;

  virtual ~TaskExcuter(){};
};
}  // namespace ray