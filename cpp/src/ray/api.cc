
#include <ray/api.h>
#include <ray/api/ray_config.h>

#include "runtime/abstract_ray_runtime.h"

namespace ray {
namespace api {

std::shared_ptr<RayRuntime> Ray::runtime_ = nullptr;

std::once_flag Ray::is_inited_;
void Ray::Init(std::string address, bool local_mode, int min_workers, int max_workers) {
  std::call_once(is_inited_, [address, local_mode, min_workers, max_workers] {
    runtime_ = AbstractRayRuntime::DoInit(
        RayConfig::GetInstance(address, local_mode, min_workers, max_workers));
  });
}

void Ray::Shutdown() { AbstractRayRuntime::DoShutdown(RayConfig::GetInstance()); }

}  // namespace api
}  // namespace ray