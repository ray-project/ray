#pragma once
#include <ray/api/ray_config.h>
#include <string>
#include "ray/core.h"

namespace ray {
namespace api {

class ProcessHelper {
 public:
  void RayStart(std::shared_ptr<RayConfig> config, CoreWorkerOptions::TaskExecutionCallback callback);
  void RayStop(std::shared_ptr<RayConfig> config);

  static std::shared_ptr<ProcessHelper> getInstance() {
    return ProcessHelper_;
  }

 private:
  static std::shared_ptr<ProcessHelper> ProcessHelper_;
};
}  // namespace api
}  // namespace ray