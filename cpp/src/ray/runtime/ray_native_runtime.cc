
#include "ray_native_runtime.h"

namespace ray {
namespace api {

RayNativeRuntime::RayNativeRuntime(std::shared_ptr<RayConfig> config) {
  config_ = config;
  worker_ =
      std::unique_ptr<WorkerContext>(new WorkerContext(WorkerType::DRIVER, JobID::Nil()));
}

}  // namespace api
}  // namespace ray