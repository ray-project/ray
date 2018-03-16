#include <iostream>

#include "ray/raylet/raylet.h"
#include "ray/status.h"

#ifndef RAYLET_TEST
int main(int argc, char *argv[]) {
  RAY_CHECK(argc == 3);

  // configure
  std::unordered_map<std::string, double> static_resource_conf;
  static_resource_conf = {{"CPU", 1}, {"GPU", 1}};
  ray::raylet::ResourceSet resource_config(std::move(static_resource_conf));
  ray::ObjectManagerConfig om_config;
  om_config.store_socket_name = std::string(argv[2]);

  //  initialize mock gcs & object directory
  auto gcs_client = std::make_shared<ray::gcs::AsyncGcsClient>();
  RAY_LOG(INFO) << "Initializing GCS client "
                << gcs_client->client_table().GetLocalClientId();

  // Initialize the node manager.
  boost::asio::io_service main_service;
  boost::asio::io_service object_manager_service;
  ray::raylet::Raylet server(main_service, object_manager_service,
                             std::string(argv[1]), resource_config, om_config,
                             gcs_client);
  main_service.run();
}
#endif
