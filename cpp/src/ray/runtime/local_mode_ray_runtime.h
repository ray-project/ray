
#pragma once

#include <unordered_map>

#include "abstract_ray_runtime.h"

namespace ray {
namespace internal {

class LocalModeRayRuntime : public AbstractRayRuntime {
 public:
  LocalModeRayRuntime();

  ActorID GetNextActorID();
};

}  // namespace internal
}  // namespace ray