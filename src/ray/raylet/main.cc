#include <iostream>

#include "ray/raylet/raylet.h"
#include "ray/status.h"

#ifndef RAYLET_TEST
int main(int argc, char *argv[]) {
  RAY_CHECK(argc == 3);

  const std::string raylet_socket_name = std::string(argv[1]);
  const std::string store_socket_name = std::string(argv[2]);

  // Configuration for the node manager.
  ray::raylet::NodeManagerConfig node_manager_config;
  std::unordered_map<std::string, double> static_resource_conf;
  static_resource_conf = {{"CPU", 1}, {"GPU", 1}};
  node_manager_config.resource_config =
      ray::raylet::ResourceSet(std::move(static_resource_conf));
  // Use a default worker that can execute empty tasks with dependencies.
  node_manager_config.worker_command.push_back("python");
  node_manager_config.worker_command.push_back("../../../src/ray/test/worker.py");
  node_manager_config.worker_command.push_back(raylet_socket_name.c_str());
  node_manager_config.worker_command.push_back(store_socket_name.c_str());

  // Configuration for the object manager.
  ray::ObjectManagerConfig om_config;
  om_config.store_socket_name = std::string(argv[2]);

  //  initialize mock gcs & object directory
  auto gcs_client = std::make_shared<ray::gcs::AsyncGcsClient>();
  RAY_LOG(INFO) << "Initializing GCS client "
                << gcs_client->client_table().GetLocalClientId();

  // Initialize the node manager.
  boost::asio::io_service main_service;
  boost::asio::io_service object_manager_service;
  ray::raylet::Raylet server(main_service, object_manager_service, std::string(argv[1]),
                             node_manager_config, om_config, gcs_client);
  main_service.run();
}
#endif
