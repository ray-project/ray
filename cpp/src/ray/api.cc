
#include <ray/api.h>

#include "config_internal.h"
#include "runtime/abstract_ray_runtime.h"

namespace ray {

void Init(ray::RayConfig &config, int *argc, char ***argv) {
  ray::internal::ConfigInternal::Instance().Init(config, argc, argv);
  Init();
}

void Init(ray::RayConfig &config) { Init(config, nullptr, nullptr); }

void Init() {
  std::call_once(is_inited_, [] {
    auto runtime = ray::internal::AbstractRayRuntime::DoInit();
    ray::internal::RayRuntimeHolder::Instance().Init(runtime);
  });
}

void Shutdown() { ray::internal::AbstractRayRuntime::DoShutdown(); }

}  // namespace ray