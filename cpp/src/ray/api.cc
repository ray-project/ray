
#include <ray/api.h>

#include <ray/api/ray_config.h>
#include "runtime/abstract_ray_runtime.h"

namespace ray {
namespace api {

RayRuntime *Ray::runtime_ = nullptr;

std::once_flag Ray::is_inited_;
void Ray::Init() {
  std::call_once(is_inited_,
                 [] { runtime_ = AbstractRayRuntime::DoInit(RayConfig::GetInstance()); });
}

}  // namespace api
}  // namespace ray