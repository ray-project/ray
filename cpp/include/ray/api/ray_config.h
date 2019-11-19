
#pragma once

#include <ray/api/ray_mode.h>

namespace ray {

class RayConfig {
 public:
  WorkerMode workerMode = WorkerMode::DRIVER;

  RunMode runMode = RunMode::SINGLE_PROCESS;
};

}  // namespace ray