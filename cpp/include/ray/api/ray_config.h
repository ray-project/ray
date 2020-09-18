
#pragma once
#include <memory>
#include <string>
#include "ray/core.h"

namespace ray {
namespace api {

enum class RunMode { SINGLE_PROCESS, CLUSTER };

/// TODO(Guyang Song): Make configuration complete and use to initialize.
class RayConfig {
 public:
  WorkerType worker_type = WorkerType::DRIVER;

  RunMode run_mode = RunMode::SINGLE_PROCESS;

  std::string redis_ip;

  int redis_port = 6379;

  std::string redis_password = "5241590000000000";

  int node_manager_port = 62665;

  std::string lib_name = "";

  std::string store_socket = "";

  std::string raylet_socket = "";

  std::string session_dir = "";

  static std::shared_ptr<RayConfig> GetInstance();

 private:
  static std::shared_ptr<RayConfig> config_;
};

}  // namespace api
}  // namespace ray