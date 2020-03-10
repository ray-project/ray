// Copyright 2017 The Ray Authors.
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

#include <iostream>

#include "ray/common/ray_config.h"
#include "ray/gcs/gcs_server/gcs_server.h"
#include "ray/util/util.h"

#include "gflags/gflags.h"

DEFINE_string(redis_address, "", "The ip address of redis.");
DEFINE_int32(redis_port, -1, "The port of redis.");
DEFINE_string(config_list, "", "The config list of raylet.");
DEFINE_string(redis_password, "", "The password of redis.");
DEFINE_bool(retry_redis, false, "Whether we retry to connect to the redis.");

int main(int argc, char *argv[]) {
  InitShutdownRAII ray_log_shutdown_raii(ray::RayLog::StartRayLog,
                                         ray::RayLog::ShutDownRayLog, argv[0],
                                         ray::RayLogLevel::INFO, /*log_dir=*/"");
  ray::RayLog::InstallFailureSignalHandler();

  gflags::ParseCommandLineFlags(&argc, &argv, true);
  const std::string redis_address = FLAGS_redis_address;
  const int redis_port = static_cast<int>(FLAGS_redis_port);
  const std::string config_list = FLAGS_config_list;
  const std::string redis_password = FLAGS_redis_password;
  const bool retry_redis = FLAGS_retry_redis;
  gflags::ShutDownCommandLineFlags();

  std::unordered_map<std::string, std::string> config_map;

  // Parse the configuration list.
  std::istringstream config_string(config_list);
  std::string config_name;
  std::string config_value;

  while (std::getline(config_string, config_name, ',')) {
    RAY_CHECK(std::getline(config_string, config_value, ','));
    config_map[config_name] = config_value;
  }

  RayConfig::instance().initialize(config_map);

  ray::gcs::GcsServerConfig gcs_server_config;
  gcs_server_config.grpc_server_name = "GcsServer";
  gcs_server_config.grpc_server_port = 0;
  gcs_server_config.grpc_server_thread_num = 1;
  gcs_server_config.redis_address = redis_address;
  gcs_server_config.redis_port = redis_port;
  gcs_server_config.redis_password = redis_password;
  gcs_server_config.retry_redis = retry_redis;
  ray::gcs::GcsServer gcs_server(gcs_server_config);
  gcs_server.Start();
}
