
#include <ray/api.h>

#include <ray/api/ray_config.h>
#include "runtime/abstract_ray_runtime.h"

namespace ray { namespace api {

RayRuntime *Ray::_impl = nullptr;

void Ray::init() { _impl = &AbstractRayRuntime::doInit(std::make_shared<RayConfig>()); }

}  }// namespace ray::api