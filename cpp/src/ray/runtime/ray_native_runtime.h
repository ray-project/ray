
#pragma once

#include "abstract_ray_runtime.h"

namespace ray {
namespace api {

class RayNativeRuntime : public AbstractRayRuntime {
  friend class AbstractRayRuntime;

 private:
  RayNativeRuntime(std::shared_ptr<RayConfig> config);
};

}  // namespace api
}  // namespace ray