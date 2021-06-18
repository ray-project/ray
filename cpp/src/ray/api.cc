
#include <ray/api.h>

#include "config_internal.h"
#include "runtime/abstract_ray_runtime.h"

namespace ray {
namespace api {

std::once_flag Ray::is_inited_;

void Ray::Init(RayConfig &config, int *argc, char ***argv) {
  ConfigInternal::Instance().Init(config, argc, argv);
  Init();
}

void Ray::Init(RayConfig &config) { Init(config, nullptr, nullptr); }

void Ray::Init() {
  std::call_once(is_inited_, [] {
    auto runtime = AbstractRayRuntime::DoInit();
    ray::internal::RayRuntimeHolder::Instance().Init(runtime);
  });
}

void Ray::Shutdown() { AbstractRayRuntime::DoShutdown(); }

}  // namespace api
}  // namespace ray