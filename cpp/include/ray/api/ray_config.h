
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

  bool use_ray_remote = false;

  static std::shared_ptr<RayConfig> GetInstance();

  void SetRedisAddress(const std::string address) {
    auto pos = address.find(':');
    RAY_CHECK(pos != std::string::npos);
    redis_ip = address.substr(0, pos);
    redis_port = std::stoi(address.substr(pos + 1, address.length()));
  }

 private:
  static std::shared_ptr<RayConfig> config_;
};

}  // namespace api
}  // namespace ray