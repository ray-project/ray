#pragma once
#include <ray/api/ray_config.h>
#include <string>
#include "ray/core.h"

namespace ray {
namespace api {

class ProcessHelper {
 public:
  void RayStart(std::shared_ptr<RayConfig> config,
                CoreWorkerOptions::TaskExecutionCallback callback);
  void RayStop(std::shared_ptr<RayConfig> config);

  static ProcessHelper &GetInstance() {
    static ProcessHelper processHelper;
    return processHelper;
  }

  ProcessHelper(ProcessHelper const &) = delete;
  void operator=(ProcessHelper const &) = delete;

 private:
  ProcessHelper(){};
};
}  // namespace api
}  // namespace ray