
#include "RayNativeRuntime.h"

namespace ray {

RayNativeRuntime::RayNativeRuntime(std::shared_ptr<RayConfig> params) {
  _params = params;
  std::unique_ptr<Worker> ptr(new Worker(params));
  _worker = std::move(ptr);
}

}  // namespace ray