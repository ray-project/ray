#include <iostream>

#include <boost/bind.hpp>

#include "common.h"
#include "node_manager.h"


/**
 * A demo that starts two Raylets, with one object store each. The two Raylets
 * share a mock GCS client for communication between the two (e.g., for
 * ObjectManager::Push).
 */
int main(int argc, char *argv[]) {
  CHECK(argc == 3);
  std::string store1 = "/tmp/store1";
  std::string store2 = "/tmp/store2";
  // start store
  std::string plasma_dir = "../../plasma";
  std::string plasma_command1 = plasma_dir + "/plasma_store -m 1000000000 -s ";
  std::string plasma_command2 = " 1> /dev/null 2> /dev/null &";
  cout << plasma_command1 << store1 << plasma_command2 << endl;
  cout << plasma_command1 << store2 << plasma_command2 << endl;
  int s;
  s = system((plasma_command1 + store1 + plasma_command2).c_str());
  CHECK(s == 0);
  s = system((plasma_command1 + store2 + plasma_command2).c_str());

  // configure
  std::unordered_map<std::string, double> static_resource_conf;
  static_resource_conf = {{"CPU", 1}, {"GPU", 1}};
  ray::ResourceSet resource_config(std::move(static_resource_conf));
  ray::OMConfig om_config;

  //  initialize mock gcs & object directory
  shared_ptr<ray::GcsClient> mock_gcs_client = shared_ptr<ray::GcsClient>(new ray::GcsClient());
  shared_ptr<ray::ObjectDirectory> od = shared_ptr<ray::ObjectDirectory>(new ray::ObjectDirectory());
  od->InitGcs(mock_gcs_client);

  // Initialize the node manager.
  boost::asio::io_service io_service;
  om_config.store_socket_name = store1;
  ray::NodeServer server1(io_service, std::string(argv[1]), resource_config, om_config, mock_gcs_client, od);
  om_config.store_socket_name = store2;
  ray::NodeServer server2(io_service, std::string(argv[2]), resource_config, om_config, mock_gcs_client, od);
  io_service.run();
  ray::Status ray_status1 = server1.Terminate();
  ray::Status ray_status2 = server2.Terminate();

  return !(ray_status1.ok() && ray_status2.ok());
}
