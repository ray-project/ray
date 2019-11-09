
#pragma once

#include <ray/api/RayMode.h>

namespace ray {

class RayConfig {
 public:
  WorkerMode workerMode = WorkerMode::DRIVER;

  RunMode runMode = RunMode::SINGLE_PROCESS;
};

}  // namespace ray