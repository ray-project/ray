
#pragma once

#include <unordered_map>

#include "abstract_ray_runtime.h"

namespace ray {
namespace api {

class LocalModeRayRuntime : public AbstractRayRuntime {
 public:
  LocalModeRayRuntime();

  ActorID GetNextActorID();
};

}  // namespace api
}  // namespace ray