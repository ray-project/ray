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
#include "ray/raylet/monitor.h"
#include "ray/util/util.h"

#include "gflags/gflags.h"

DEFINE_string(redis_address, "", "The ip address of redis.");
DEFINE_int32(redis_port, -1, "The port of redis.");
DEFINE_string(config_list, "", "The config list of raylet.");
DEFINE_string(redis_password, "", "The password of redis.");

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
  gflags::ShutDownCommandLineFlags();

  ray::gcs::GcsClientOptions gcs_client_options(redis_address, redis_port,
                                                redis_password);

  std::unordered_map<std::string, std::string> raylet_config;

  // Parse the configuration list.
  std::istringstream config_string(config_list);
  std::string config_name;
  std::string config_value;

  while (std::getline(config_string, config_name, ',')) {
    RAY_CHECK(std::getline(config_string, config_value, ','));
    // TODO(rkn): The line below could throw an exception. What should we do about this?
    raylet_config[config_name] = config_value;
  }

  RayConfig::instance().initialize(raylet_config);

  boost::asio::io_service io_service;

  // The code below is commented out because it appears to introduce a double
  // free error in the raylet monitor.
  // // Destroy the Raylet monitor on a SIGTERM. The pointer to io_service is
  // // guaranteed to be valid since this function will run the event loop
  // // instead of returning immediately.
  // auto handler = [&io_service](const boost::system::error_code &error,
  //                              int signal_number) { io_service.stop(); };
  // boost::asio::signal_set signals(io_service);
  // #ifdef _WIN32
  //   signals.add(SIGBREAK);
  // #else
  //   signals.add(SIGTERM);
  // #endif
  // signals.async_wait(handler);

  // Initialize the monitor.
  ray::raylet::Monitor monitor(io_service, gcs_client_options);
  monitor.Start();
  io_service.run();
}
