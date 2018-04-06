#include <iostream>

#include "ray/raylet/monitor.h"

int main(int argc, char *argv[]) {
  RAY_CHECK(argc == 3);

  const std::string redis_address = std::string(argv[1]);
  int redis_port = std::stoi(argv[2]);

  // Initialize the monitor.
  boost::asio::io_service io_service;
  ray::raylet::Monitor monitor(io_service, redis_address, redis_port);
  monitor.Start();
  io_service.run();
}
