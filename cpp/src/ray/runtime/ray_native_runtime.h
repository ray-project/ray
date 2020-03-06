
#pragma once

#include "abstract_ray_runtime.h"

namespace ray {

class RayNativeRuntime : public AbstractRayRuntime {
  friend class AbstractRayRuntime;

 private:
  RayNativeRuntime(std::shared_ptr<RayConfig> config);
};

}  // namespace ray