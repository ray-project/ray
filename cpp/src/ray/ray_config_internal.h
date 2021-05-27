
#pragma once
#include <ray/api/ray_config.h>
#include <memory>
#include <string>
#include "ray/core_worker/common.h"

namespace ray {
namespace api {

enum class RunMode { SINGLE_PROCESS, CLUSTER };

class RayConfigInternal {
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

  std::string job_id = "";

  std::string logs_dir = "";

  static std::shared_ptr<RayConfigInternal> GetInstance();

  void Init(RayConfig &config);

  void SetRedisAddress(const std::string address);

 private:
  static std::shared_ptr<RayConfigInternal> config_;
};

}  // namespace api
}  // namespace ray