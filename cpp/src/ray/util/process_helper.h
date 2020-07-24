#pragma once
#include <ray/api/ray_config.h>
#include <string>
#include "ray/core.h"

namespace ray {
namespace api {

class ProcessHelper {
 public:
  void RayStart(std::shared_ptr<RayConfig> config);
  void RayStop(std::shared_ptr<RayConfig> config);
  static std::shared_ptr<ProcessHelper> getInstance() {
    if (ProcessHelper_ == nullptr) {
      ProcessHelper_ = std::shared_ptr<ProcessHelper>(new ProcessHelper);
    }
    return ProcessHelper_;
  }

 private:
  static std::shared_ptr<ProcessHelper> ProcessHelper_;
};
}  // namespace api
}  // namespace ray