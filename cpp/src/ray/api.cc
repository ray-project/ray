
#include <ray/api.h>

#include <ray/api/ray_config.h>
#include "runtime/abstract_ray_runtime.h"

namespace ray {
namespace api {

RayRuntime *Ray::runtime_ = nullptr;

void Ray::Init() {
  runtime_ = &AbstractRayRuntime::DoInit(std::make_shared<RayConfig>());
}

}  // namespace api
}  // namespace ray