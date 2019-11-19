
#include <ray/api.h>

#include <ray/api/ray_config.h>
#include "runtime/ray_runtime.h"

namespace ray {

RayApi *Ray::_impl = nullptr;

void Ray::init() { _impl = &RayRuntime::doInit(std::make_shared<RayConfig>()); }

}  // namespace ray