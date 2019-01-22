#include <iostream>

#include "ray/ray_config.h"
#include "ray/raylet/monitor.h"
#include "ray/util/util.h"

int main(int argc, char *argv[]) {
  InitShutdownRAII ray_log_shutdown_raii(ray::RayLog::StartRayLog,
                                         ray::RayLog::ShutDownRayLog, argv[0],
                                         ray::RayLogLevel::INFO, /*log_dir=*/"");
  ray::RayLog::InstallFailureSignalHandler();
  RAY_CHECK(argc == 4 || argc == 5);

  const std::string redis_address = std::string(argv[1]);
  int redis_port = std::stoi(argv[2]);
  const std::string config_list = std::string(argv[3]);
  const std::string redis_password = (argc == 5 ? std::string(argv[4]) : "");

  std::unordered_map<std::string, int> raylet_config;

  // Parse the configuration list.
  std::istringstream config_string(config_list);
  std::string config_name;
  std::string config_value;

  while (std::getline(config_string, config_name, ',')) {
    RAY_CHECK(std::getline(config_string, config_value, ','));
    // TODO(rkn): The line below could throw an exception. What should we do about this?
    raylet_config[config_name] = std::stoi(config_value);
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
  // boost::asio::signal_set signals(io_service, SIGTERM);
  // signals.async_wait(handler);

  // Initialize the monitor.
  ray::raylet::Monitor monitor(io_service, redis_address, redis_port, redis_password);
  monitor.Start();
  io_service.run();
}
