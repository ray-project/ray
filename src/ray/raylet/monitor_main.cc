#include <iostream>

#include "ray/raylet/monitor.h"
#include "ray/util/signal_handler.h"
#include "ray/util/util.h"

int main(int argc, char *argv[]) {
  DefaultInitShutdown ray_log_shutdown_wrapper(
      RayLog::StartRayLog, RayLog::ShutDownRayLog, argv[0], RAY_INFO, "");
  DefaultInitShutdown signal_handler_uninstall_wrapper(
      SignalHandlers::InstallSignalHandler, SignalHandlers::UninstallSignalHandler,
      argv[0], true);
  RAY_CHECK(argc == 3);

  const std::string redis_address = std::string(argv[1]);
  int redis_port = std::stoi(argv[2]);

  // Initialize the monitor.
  boost::asio::io_service io_service;
  ray::raylet::Monitor monitor(io_service, redis_address, redis_port);
  monitor.Start();
  io_service.run();
}
