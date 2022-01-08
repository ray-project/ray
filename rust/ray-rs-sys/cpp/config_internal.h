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

#include <memory>
#include <string>

#include "ray/core_worker/common.h"
#include <boost/optional.hpp>
#include <boost/filesystem.hpp>

namespace ray {
class RayConfig {
 public:
  // The address of the Ray cluster to connect to.
  // If not provided, it will be initialized from environment variable "RAY_ADDRESS" by
  // default.
  std::string address = "";

  // Whether or not to run this application in a local mode. This is used for debugging.
  bool local_mode = false;

  // An array of directories or dynamic library files that specify the search path for
  // user code. This parameter is not used when the application runs in local mode.
  // Only searching the top level under a directory.
  std::vector<std::string> code_search_path;

  // The command line args to be appended as parameters of the `ray start` command. It
  // takes effect only if Ray head is started by a driver. Run `ray start --help` for
  // details.
  std::vector<std::string> head_args = {};

  /* The following are unstable parameters and their use is discouraged. */

  // Prevents external clients without the password from connecting to Redis if provided.
  boost::optional<std::string> redis_password_;
};


namespace internal {

using ray::core::WorkerType;

enum class RunMode { SINGLE_PROCESS, CLUSTER };


class ConfigInternal {
 public:
  WorkerType worker_type = WorkerType::DRIVER;

  RunMode run_mode = RunMode::SINGLE_PROCESS;

  std::string redis_ip;

  int redis_port = 6379;

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

  static ConfigInternal &Instance() {
    static ConfigInternal config;
    return config;
  };

  void Init(RayConfig &config, int argc, char **argv);

  void SetRedisAddress(const std::string address);

  ConfigInternal(ConfigInternal const &) = delete;

  void operator=(ConfigInternal const &) = delete;

 private:
  ConfigInternal(){};
};

}  // namespace internal
}  // namespace ray
