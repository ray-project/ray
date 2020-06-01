
#pragma once

#include <unordered_map>
#include "abstract_ray_runtime.h"
#include "ray/core.h"

namespace ray {
namespace api {

class LocalModeRayRuntime : public AbstractRayRuntime {
 public:
  LocalModeRayRuntime(std::shared_ptr<RayConfig> config);
};

}  // namespace api
}  // namespace ray