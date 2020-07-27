
#pragma once
#include <memory>
#include <string>

namespace ray {
namespace api {

enum class RunMode { SINGLE_PROCESS, SINGLE_BOX, CLUSTER };

enum class WorkerMode { NONE, DRIVER, WORKER };

/// TODO(Guyang Song): Make configuration complete and use to initialize.
class RayConfig {
 public:
  WorkerMode worker_mode = WorkerMode::DRIVER;

  RunMode run_mode = RunMode::SINGLE_PROCESS;

  std::string redis_address;

  static std::shared_ptr<RayConfig> GetInstance();

 private:
  static std::shared_ptr<RayConfig> config_;
};

}  // namespace api
}  // namespace ray