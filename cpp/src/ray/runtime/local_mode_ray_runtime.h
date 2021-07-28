
#pragma once

#include <unordered_map>

#include "abstract_ray_runtime.h"

namespace ray {
namespace runtime {

class LocalModeRayRuntime : public AbstractRayRuntime {
 public:
  LocalModeRayRuntime();

  ActorID GetNextActorID();
};

}  // namespace runtime
}  // namespace ray