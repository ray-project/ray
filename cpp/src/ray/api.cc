
#include <ray/api.h>
#include <ray/api/ray_config.h>

#include "runtime/abstract_ray_runtime.h"

namespace ray {
namespace api {

std::once_flag Ray::is_inited_;
void Ray::Init() {
  std::call_once(is_inited_, [] {
    auto runtime = AbstractRayRuntime::DoInit(RayConfig::GetInstance());
    RayRuntimeHolder::Instance().Init(runtime);
  });
}

void Ray::Shutdown() { AbstractRayRuntime::DoShutdown(RayConfig::GetInstance()); }

}  // namespace api
}  // namespace ray