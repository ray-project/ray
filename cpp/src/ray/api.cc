
#include <ray/api.h>

#include <ray/api/ray_config.h>
#include "runtime/abstract_ray_runtime.h"

namespace ray {
namespace api {

RayRuntime *Ray::impl_ = nullptr;

void Ray::Init() { impl_ = &AbstractRayRuntime::DoInit(std::make_shared<RayConfig>()); }

}  // namespace api
}  // namespace ray