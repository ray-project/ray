#include <iostream>

#include <boost/bind.hpp>

#include "common.h"
#include "node_manager.h"

#ifndef NODE_MANAGER_TEST
int main(int argc, char *argv[]) {
  CHECK(argc == 2);

  boost::asio::io_service io_service;
  std::unordered_map<std::string, double> static_resource_conf;
  static_resource_conf = {{"num_cpus", 1}, {"num_gpus", 1}};
  ray::ResourceSet resource_config(static_resource_conf);

  // Initialize the node manager.
  ray::NodeServer server(io_service, std::string(argv[1]), resource_config);
  io_service.run();
  return 0;
}
#endif
