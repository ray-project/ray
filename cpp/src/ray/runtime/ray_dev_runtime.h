
#pragma once

#include <ray/core.h>
#include <unordered_map>
#include "abstract_ray_runtime.h"

namespace ray {
namespace api {

class RayDevRuntime : public AbstractRayRuntime {
  friend class AbstractRayRuntime;

 private:
  RayDevRuntime(std::shared_ptr<RayConfig> config);

  ActorID CreateActor(remote_function_ptr_holder &fptr,
                      std::shared_ptr<msgpack::sbuffer> args);
};

}  // namespace api
}  // namespace ray