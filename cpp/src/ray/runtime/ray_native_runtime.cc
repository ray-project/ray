
#include "ray_native_runtime.h"

namespace ray {

RayNativeRuntime::RayNativeRuntime(std::shared_ptr<RayConfig> config) {
  _config = config;
  _worker = std::unique_ptr<Worker>(new Worker(config));
}

}  // namespace ray