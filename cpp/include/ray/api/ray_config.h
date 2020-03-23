
#pragma once

namespace ray {
namespace api {

enum class RunMode { SINGLE_PROCESS, SINGLE_BOX, CLUSTER };

enum class WorkerMode { NONE, DRIVER, WORKER };

/// TODO(Guyang Song): Make configuration complete and use to initialize.
class RayConfig {
 public:
  WorkerMode workerMode = WorkerMode::DRIVER;

  RunMode runMode = RunMode::SINGLE_PROCESS;
};

}  // namespace api
}  // namespace ray