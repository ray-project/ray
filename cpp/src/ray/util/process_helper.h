#pragma once
#include <string>
#include "../ray_config_internal.h"
#include "ray/core.h"

namespace ray {
namespace api {

class ProcessHelper {
 public:
  void RayStart(std::shared_ptr<RayConfigInternal> config,
                CoreWorkerOptions::TaskExecutionCallback callback);
  void RayStop(std::shared_ptr<RayConfigInternal> config);

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