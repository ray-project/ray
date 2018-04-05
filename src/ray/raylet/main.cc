#include <iostream>

#include "ray/raylet/raylet.h"
#include "ray/status.h"

#ifndef RAYLET_TEST
int main(int argc, char *argv[]) {
  RAY_CHECK(argc == 7);

  const std::string raylet_socket_name = std::string(argv[1]);
  const std::string store_socket_name = std::string(argv[2]);
  const std::string node_ip_address = std::string(argv[3]);
  const std::string redis_address = std::string(argv[4]);
  int redis_port = std::stoi(argv[5]);
  const std::string worker_command = std::string(argv[6]);

  // Configuration for the node manager.
  ray::raylet::NodeManagerConfig node_manager_config;
  std::unordered_map<std::string, double> static_resource_conf;
  static_resource_conf = {{"CPU", 1}, {"GPU", 1}};
  node_manager_config.resource_config =
      ray::raylet::ResourceSet(std::move(static_resource_conf));
  node_manager_config.num_initial_workers = 0;
  // Use a default worker that can execute empty tasks with dependencies.

  std::stringstream worker_command_stream(worker_command);
  std::string token;
  while (getline(worker_command_stream, token, ' ')) {
    node_manager_config.worker_command.push_back(token);
  }

  node_manager_config.heartbeat_period_ms =
      RayConfig::instance().heartbeat_timeout_milliseconds();

  // Configuration for the object manager.
  ray::ObjectManagerConfig object_manager_config;
  object_manager_config.store_socket_name = store_socket_name;

  //  initialize mock gcs & object directory
  auto gcs_client = std::make_shared<ray::gcs::AsyncGcsClient>();
  RAY_LOG(INFO) << "Initializing GCS client "
                << gcs_client->client_table().GetLocalClientId();

  // Initialize the node manager.
  boost::asio::io_service main_service;
  std::unique_ptr<boost::asio::io_service> object_manager_service;

  object_manager_service.reset(new boost::asio::io_service());
  ray::raylet::Raylet server(main_service, std::move(object_manager_service),
                             raylet_socket_name, node_ip_address, redis_address,
                             redis_port, node_manager_config, object_manager_config,
                             gcs_client);
  main_service.run();
}
#endif
