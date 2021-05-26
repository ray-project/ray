
#include <ray/api.h>

#include "ray_config_internal.h"
#include "runtime/abstract_ray_runtime.h"

namespace ray {
namespace api {

std::once_flag Ray::is_inited_;

void Ray::Init(RayConfig &config) {
  RayConfigInternal::GetInstance()->Init(config);
  Init();
}

void Ray::Init() {
  std::call_once(is_inited_, [] {
    auto runtime = AbstractRayRuntime::DoInit(RayConfigInternal::GetInstance());
    ray::internal::RayRuntimeHolder::Instance().Init(runtime);
  });
}

void Ray::Shutdown() { AbstractRayRuntime::DoShutdown(RayConfigInternal::GetInstance()); }

}  // namespace api
}  // namespace ray