
#include <ray/api.h>

#include <ray/api/RayConfig.h>
#include "runtime/RayRuntime.h"

namespace ray {

RayApi *Ray::_impl = nullptr;

void Ray::init() { _impl = &RayRuntime::doInit(std::make_shared<RayConfig>()); }

}  // namespace ray