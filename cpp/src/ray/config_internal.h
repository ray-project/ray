// Copyright 2021 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once
#include <ray/api/ray_config.h>

#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>

#include "ray/core_worker/common.h"

namespace ray {
namespace internal {

using ray::core::WorkerType;

enum class RunMode { SINGLE_PROCESS, CLUSTER };

class ConfigInternal {
 public:
  WorkerType worker_type = WorkerType::DRIVER;

  RunMode run_mode = RunMode::SINGLE_PROCESS;

  std::string bootstrap_ip;

  int bootstrap_port = 6379;

  std::string redis_password = "5241590000000000";

  int node_manager_port = 0;

  std::vector<std::string> code_search_path;

  std::string plasma_store_socket_name = "";

  std::string raylet_socket_name = "";

  std::string session_dir = "";

  std::string job_id = "";

  std::string logs_dir = "";

  std::string node_ip_address = "";

  StartupToken startup_token;

  std::vector<std::string> head_args = {};

  boost::optional<RuntimeEnv> runtime_env;

  int runtime_env_hash = 0;

  // The default actor lifetime type.
  rpc::JobConfig_ActorLifetime default_actor_lifetime =
      rpc::JobConfig_ActorLifetime_NON_DETACHED;

  std::unordered_map<std::string, std::string> job_config_metadata;

  std::string ray_namespace = "";

  static ConfigInternal &Instance() {
    static ConfigInternal config;
    return config;
  };

  void Init(RayConfig &config, int argc, char **argv);

  void SetBootstrapAddress(std::string_view address);

  void UpdateSessionDir(const std::string dir);

  ConfigInternal(ConfigInternal const &) = delete;

  void operator=(ConfigInternal const &) = delete;

 private:
  ConfigInternal(){};
};

}  // namespace internal
}  // namespace ray
