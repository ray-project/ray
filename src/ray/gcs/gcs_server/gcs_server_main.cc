#include <iostream>

#include "ray/common/ray_config.h"
#include "ray/gcs/gcs_server/gcs_server.h"
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

  std::unordered_map<std::string, std::string> config_map;

  // Parse the configuration list.
  std::istringstream config_string(config_list);
  std::string config_name;
  std::string config_value;

  while (std::getline(config_string, config_name, ',')) {
    RAY_CHECK(std::getline(config_string, config_value, ','));
    // TODO(rkn): The line below could throw an exception. What should we do about this?
    config_map[config_name] = config_value;
  }

  RayConfig::instance().initialize(config_map);

  ray::gcs::GcsServerConfig gcs_server_config;
  gcs_server_config.server_name = "GcsServer";
  gcs_server_config.server_port = 0;
  gcs_server_config.server_thread_num = 1;
  // TODO(hc): fill gcs server config
  ray::gcs::GcsServer gcs_server(gcs_server_config);
  gcs_server.Start();
}
