
#pragma once

#include <unordered_map>

#include "abstract_ray_runtime.h"

namespace ray {
namespace api {

class NativeRayRuntime : public AbstractRayRuntime {
 public:
  NativeRayRuntime(std::shared_ptr<RayConfigInternal> config);
};

}  // namespace api
}  // namespace ray