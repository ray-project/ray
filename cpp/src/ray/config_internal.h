
#pragma once
#include <ray/api/ray_config.h>
#include <memory>
#include <string>
#include "ray/core_worker/common.h"

namespace ray {
namespace api {

enum class RunMode { SINGLE_PROCESS, CLUSTER };

class ConfigInternal {
 public:
  WorkerType worker_type = WorkerType::DRIVER;

  RunMode run_mode = RunMode::SINGLE_PROCESS;

  std::string redis_ip;

  int redis_port = 6379;

  std::string redis_password = "5241590000000000";

  int node_manager_port = 62665;

  std::string dynamic_library_path = "";

  std::string plasma_store_socket_name = "";

  std::string raylet_socket_name = "";

  std::string session_dir = "";

  std::string job_id = "";

  std::string logs_dir = "";

  std::string node_ip_address = "";

  static ConfigInternal &Instance() {
    static ConfigInternal config;
    return config;
  };

  void Init(RayConfig &config, int *argc, char ***argv);

  void SetRedisAddress(const std::string address);

  ConfigInternal(ConfigInternal const &) = delete;

  void operator=(ConfigInternal const &) = delete;

 private:
  ConfigInternal(){};
};

}  // namespace api
}  // namespace ray