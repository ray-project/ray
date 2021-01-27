// Copyright 2017 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <iostream>

#include "gflags/gflags.h"
#include "ray/common/ray_config.h"
#include "ray/gcs/gcs_server/gcs_server.h"
#include "ray/gcs/store_client/redis_store_client.h"
#include "ray/stats/stats.h"
#include "ray/util/util.h"
#include "src/ray/protobuf/gcs_service.pb.h"

DEFINE_string(redis_address, "", "The ip address of redis.");
DEFINE_int32(redis_port, -1, "The port of redis.");
DEFINE_int32(gcs_server_port, 0, "The port of gcs server.");
DEFINE_int32(metrics_agent_port, -1, "The port of metrics agent.");
DEFINE_string(config_list, "", "The config list of raylet.");
DEFINE_string(redis_password, "", "The password of redis.");
DEFINE_bool(retry_redis, false, "Whether we retry to connect to the redis.");
DEFINE_string(node_ip_address, "", "The ip address of the node.");

int main(int argc, char *argv[]) {
  InitShutdownRAII ray_log_shutdown_raii(ray::RayLog::StartRayLog,
                                         ray::RayLog::ShutDownRayLog, argv[0],
                                         ray::RayLogLevel::INFO, /*log_dir=*/"");
  ray::RayLog::InstallFailureSignalHandler();

  gflags::ParseCommandLineFlags(&argc, &argv, true);
  const std::string redis_address = FLAGS_redis_address;
  const int redis_port = static_cast<int>(FLAGS_redis_port);
  const int gcs_server_port = static_cast<int>(FLAGS_gcs_server_port);
  const int metrics_agent_port = static_cast<int>(FLAGS_metrics_agent_port);
  const std::string config_list = FLAGS_config_list;
  const std::string redis_password = FLAGS_redis_password;
  const bool retry_redis = FLAGS_retry_redis;
  const std::string node_ip_address = FLAGS_node_ip_address;
  gflags::ShutDownCommandLineFlags();

  auto promise =
      std::make_shared<std::promise<std::unordered_map<std::string, std::string>>>();
  std::thread([=] {
    boost::asio::io_service service;

    // Init backend client.
    ray::gcs::RedisClientOptions redis_client_options(redis_address, redis_port,
                                                      redis_password);
    auto redis_client = std::make_shared<ray::gcs::RedisClient>(redis_client_options);
    auto status = redis_client->Connect(service);
    RAY_CHECK(status.ok()) << "Failed to init redis gcs client as " << status;

    // Init storage.
    auto storage = std::make_shared<ray::gcs::RedisGcsTableStorage>(redis_client);

    // Parse the configuration list.
    std::unordered_map<std::string, std::string> config;

    std::istringstream config_string(config_list);
    std::string config_name;
    std::string config_value;

    while (std::getline(config_string, config_name, ',')) {
      RAY_CHECK(std::getline(config_string, config_value, ';'));
      config[config_name] = config_value;
    }

    // The internal_config is only set on the gcs--other nodes get it from GCS.
    ray::rpc::SetInternalConfigRequest request;
    request.mutable_config()->mutable_config()->insert(config.begin(), config.end());

    auto on_done = [promise, &service, &config](const ray::Status &status) {
      promise->set_value(config);
      service.stop();
    };
    RAY_CHECK_OK(storage->InternalConfigTable().Put(ray::UniqueID::Nil(),
                                                    request.config(), on_done));
    boost::asio::io_service::work work(service);
    service.run();
  })
      .detach();
  auto config = promise->get_future().get();

  RayConfig::instance().initialize(config);
  const ray::stats::TagsType global_tags = {
      {ray::stats::ComponentKey, "gcs_server"},
      {ray::stats::VersionKey, "2.0.0.dev0"},
      {ray::stats::NodeAddressKey, node_ip_address}};
  ray::stats::Init(global_tags, metrics_agent_port);

  // IO Service for main loop.
  boost::asio::io_service main_service;
  // Ensure that the IO service keeps running. Without this, the main_service will exit
  // as soon as there is no more work to be processed.
  boost::asio::io_service::work work(main_service);

  ray::gcs::GcsServerConfig gcs_server_config;
  gcs_server_config.grpc_server_name = "GcsServer";
  gcs_server_config.grpc_server_port = gcs_server_port;
  gcs_server_config.grpc_server_thread_num =
      RayConfig::instance().gcs_server_rpc_server_thread_num();
  gcs_server_config.redis_address = redis_address;
  gcs_server_config.redis_port = redis_port;
  gcs_server_config.redis_password = redis_password;
  gcs_server_config.retry_redis = retry_redis;
  gcs_server_config.node_ip_address = node_ip_address;
  ray::gcs::GcsServer gcs_server(gcs_server_config, main_service);

  // Destroy the GCS server on a SIGTERM. The pointer to main_service is
  // guaranteed to be valid since this function will run the event loop
  // instead of returning immediately.
  auto handler = [&main_service, &gcs_server](const boost::system::error_code &error,
                                              int signal_number) {
    RAY_LOG(INFO) << "GCS server received SIGTERM, shutting down...";
    gcs_server.Stop();
    ray::stats::Shutdown();
    main_service.stop();
  };
  boost::asio::signal_set signals(main_service);
#ifdef _WIN32
  signals.add(SIGBREAK);
#else
  signals.add(SIGTERM);
#endif
  signals.async_wait(handler);

  gcs_server.Start();

  main_service.run();
}
