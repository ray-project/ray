
#pragma once

#include <unordered_map>

#include "abstract_ray_runtime.h"
#include "ray/core.h"

namespace ray {
namespace api {

class NativeRayRuntime : public AbstractRayRuntime {
 public:
  NativeRayRuntime(std::shared_ptr<RayConfig> config);
};

}  // namespace api
}  // namespace ray