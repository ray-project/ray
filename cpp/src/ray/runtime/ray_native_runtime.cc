
#include "ray_native_runtime.h"

namespace ray { namespace api {

RayNativeRuntime::RayNativeRuntime(std::shared_ptr<RayConfig> config) {
  _config = config;
  _worker = std::unique_ptr<WorkerContext>(new WorkerContext(WorkerType::DRIVER, JobID::Nil()));
}

}  }// namespace ray::api