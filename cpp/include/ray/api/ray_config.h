
#pragma once

#include <ray/api/ray_mode.h>

namespace ray { namespace api {

class RayConfig {
 public:
  WorkerMode workerMode = WorkerMode::DRIVER;

  RunMode runMode = RunMode::SINGLE_PROCESS;
};

}  }// namespace ray::api