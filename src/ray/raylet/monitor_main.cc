#include <iostream>

#include "ray/raylet/monitor.h"
#include "ray/util/util.h"

int main(int argc, char *argv[]) {
  InitShutdownRAII ray_log_shutdown_raii(ray::RayLog::StartRayLog,
                                         ray::RayLog::ShutDownRayLog, argv[0], RAY_INFO,
                                         /*log_dir=*/"");
  ray::RayLog::InstallFailureSignalHandler();
  RAY_CHECK(argc == 3 || argc == 4);

  const std::string redis_address = std::string(argv[1]);
  int redis_port = std::stoi(argv[2]);

  std::string redis_password = "";
  if (argc == 4) {
    redis_password = std::string(argv[3]);
  }

  // Initialize the monitor.
  boost::asio::io_service io_service;
  ray::raylet::Monitor monitor(io_service, redis_address, redis_port, redis_password);
  monitor.Start();
  io_service.run();
}
