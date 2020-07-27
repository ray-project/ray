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
#include "ray/common/id.h"
#include "ray/common/ray_config.h"
#include "ray/common/status.h"
#include "ray/common/task/task_common.h"
#include "ray/gcs/gcs_client/service_based_gcs_client.h"
#include "ray/raylet/raylet.h"
#include "ray/stats/stats.h"

DEFINE_string(raylet_socket_name, "", "The socket name of raylet.");
DEFINE_string(store_socket_name, "", "The socket name of object store.");
DEFINE_int32(object_manager_port, -1, "The port of object manager.");
DEFINE_int32(node_manager_port, -1, "The port of node manager.");
DEFINE_int32(metrics_agent_port, -1, "The port of metrics agent.");
DEFINE_string(node_ip_address, "", "The ip address of this node.");
DEFINE_string(redis_address, "", "The ip address of redis server.");
DEFINE_int32(redis_port, -1, "The port of redis server.");
DEFINE_int32(min_worker_port, 0,
             "The lowest port that workers' gRPC servers will bind on.");
DEFINE_int32(max_worker_port, 0,
             "The highest port that workers' gRPC servers will bind on.");
DEFINE_int32(num_initial_workers, 0, "Number of initial workers.");
DEFINE_int32(maximum_startup_concurrency, 1, "Maximum startup concurrency");
DEFINE_string(static_resource_list, "", "The static resource list of this node.");
DEFINE_string(config_list, "", "The raylet config list of this node.");
DEFINE_string(python_worker_command, "", "Python worker command.");
DEFINE_string(java_worker_command, "", "Java worker command.");
DEFINE_string(redis_password, "", "The password of redis.");
DEFINE_string(temp_dir, "", "Temporary directory.");
DEFINE_string(session_dir, "", "The path of this ray session directory.");
DEFINE_bool(head_node, false, "Whether this is the head node of the cluster.");
// store options
DEFINE_int64(object_store_memory, -1, "The initial memory of the object store.");
DEFINE_string(plasma_directory, "", "The shared memory directory of the object store.");
DEFINE_bool(huge_pages, false, "Whether enable huge pages");

#ifndef RAYLET_TEST

int main(int argc, char *argv[]) {
  InitShutdownRAII ray_log_shutdown_raii(ray::RayLog::StartRayLog,
                                         ray::RayLog::ShutDownRayLog, argv[0],
                                         ray::RayLogLevel::INFO,
                                         /*log_dir=*/"");
  ray::RayLog::InstallFailureSignalHandler();

  gflags::ParseCommandLineFlags(&argc, &argv, true);
  const std::string raylet_socket_name = FLAGS_raylet_socket_name;
  const std::string store_socket_name = FLAGS_store_socket_name;
  const int object_manager_port = static_cast<int>(FLAGS_object_manager_port);
  const int node_manager_port = static_cast<int>(FLAGS_node_manager_port);
  const int metrics_agent_port = static_cast<int>(FLAGS_metrics_agent_port);
  const std::string node_ip_address = FLAGS_node_ip_address;
  const std::string redis_address = FLAGS_redis_address;
  const int redis_port = static_cast<int>(FLAGS_redis_port);
  const int min_worker_port = static_cast<int>(FLAGS_min_worker_port);
  const int max_worker_port = static_cast<int>(FLAGS_max_worker_port);
  const int num_initial_workers = static_cast<int>(FLAGS_num_initial_workers);
  const int maximum_startup_concurrency =
      static_cast<int>(FLAGS_maximum_startup_concurrency);
  const std::string static_resource_list = FLAGS_static_resource_list;
  const std::string config_list = FLAGS_config_list;
  const std::string python_worker_command = FLAGS_python_worker_command;
  const std::string java_worker_command = FLAGS_java_worker_command;
  const std::string redis_password = FLAGS_redis_password;
  const std::string temp_dir = FLAGS_temp_dir;
  const std::string session_dir = FLAGS_session_dir;
  const bool head_node = FLAGS_head_node;
  const int64_t object_store_memory = FLAGS_object_store_memory;
  const std::string plasma_directory = FLAGS_plasma_directory;
  const bool huge_pages = FLAGS_huge_pages;
  gflags::ShutDownCommandLineFlags();

  // Configuration for the node manager.
  ray::raylet::NodeManagerConfig node_manager_config;
  std::unordered_map<std::string, double> static_resource_conf;
  std::unordered_map<std::string, std::string> raylet_config;

  // IO Service for node manager.
  boost::asio::io_service main_service;

  // Ensure that the IO service keeps running. Without this, the service will exit as soon
  // as there is no more work to be processed.
  boost::asio::io_service::work main_work(main_service);

  // Initialize gcs client
  ray::gcs::GcsClientOptions client_options(redis_address, redis_port, redis_password);
  std::shared_ptr<ray::gcs::GcsClient> gcs_client;

  gcs_client = std::make_shared<ray::gcs::ServiceBasedGcsClient>(client_options);

  RAY_CHECK_OK(gcs_client->Connect(main_service));

  // The internal_config is only set on the head node--other nodes get it from GCS.
  if (head_node) {
    // Parse the configuration list.
    std::istringstream config_string(config_list);
    std::string config_name;
    std::string config_value;

    while (std::getline(config_string, config_name, ',')) {
      RAY_CHECK(std::getline(config_string, config_value, ','));
      // TODO(rkn): The line below could throw an exception. What should we do about this?
      raylet_config[config_name] = config_value;
    }
    RAY_CHECK_OK(gcs_client->Nodes().AsyncSetInternalConfig(raylet_config));
  }

  std::unique_ptr<ray::raylet::Raylet> server(nullptr);

  RAY_CHECK_OK(gcs_client->Nodes().AsyncGetInternalConfig(
      [&](::ray::Status status,
          const boost::optional<std::unordered_map<std::string, std::string>>
              stored_raylet_config) {
        // NOTE: We update the raylet_config map from above. This avoids a race
        // condition between AsyncSetInternalConfig and AsyncGetInternalConfig on the
        // head node. There is an unlikely race condition where a second node calls
        // AsyncGetInternalConfig before the head finishes AsyncSetInternalConfig.
        RAY_CHECK_OK(status);
        if (stored_raylet_config.has_value()) {
          for (auto pair : stored_raylet_config.get()) {
            raylet_config[pair.first] = pair.second;
          }
        }

        RayConfig::instance().initialize(raylet_config);

        // Parse the resource list.
        std::istringstream resource_string(static_resource_list);
        std::string resource_name;
        std::string resource_quantity;

        while (std::getline(resource_string, resource_name, ',')) {
          RAY_CHECK(std::getline(resource_string, resource_quantity, ','));
          // TODO(rkn): The line below could throw an exception. What should we do
          // about this?
          static_resource_conf[resource_name] = std::stod(resource_quantity);
        }

        node_manager_config.raylet_config = raylet_config;
        node_manager_config.resource_config =
            ray::ResourceSet(std::move(static_resource_conf));
        RAY_LOG(DEBUG) << "Starting raylet with static resource configuration: "
                       << node_manager_config.resource_config.ToString();
        node_manager_config.node_manager_address = node_ip_address;
        node_manager_config.node_manager_port = node_manager_port;
        node_manager_config.num_initial_workers = num_initial_workers;
        node_manager_config.maximum_startup_concurrency = maximum_startup_concurrency;
        node_manager_config.min_worker_port = min_worker_port;
        node_manager_config.max_worker_port = max_worker_port;

        if (!python_worker_command.empty()) {
          node_manager_config.worker_commands.emplace(
              make_pair(ray::Language::PYTHON, ParseCommandLine(python_worker_command)));
        }
        if (!java_worker_command.empty()) {
          node_manager_config.worker_commands.emplace(
              make_pair(ray::Language::JAVA, ParseCommandLine(java_worker_command)));
        }
        if (python_worker_command.empty() && java_worker_command.empty()) {
          RAY_CHECK(0) << "Either Python worker command or Java worker command should be "
                          "provided.";
        }

        node_manager_config.heartbeat_period_ms =
            RayConfig::instance().raylet_heartbeat_timeout_milliseconds();
        node_manager_config.debug_dump_period_ms =
            RayConfig::instance().debug_dump_period_milliseconds();
        node_manager_config.free_objects_period_ms =
            RayConfig::instance().free_objects_period_milliseconds();
        node_manager_config.fair_queueing_enabled =
            RayConfig::instance().fair_queueing_enabled();
        node_manager_config.object_pinning_enabled =
            RayConfig::instance().object_pinning_enabled();
        node_manager_config.max_lineage_size = RayConfig::instance().max_lineage_size();
        node_manager_config.store_socket_name = store_socket_name;
        node_manager_config.temp_dir = temp_dir;
        node_manager_config.session_dir = session_dir;

        // Configuration for the object manager.
        ray::ObjectManagerConfig object_manager_config;
        object_manager_config.object_manager_port = object_manager_port;
        object_manager_config.store_socket_name = store_socket_name;
        object_manager_config.pull_timeout_ms =
            RayConfig::instance().object_manager_pull_timeout_ms();
        object_manager_config.push_timeout_ms =
            RayConfig::instance().object_manager_push_timeout_ms();
        object_manager_config.object_store_memory = object_store_memory;
        object_manager_config.plasma_directory = plasma_directory;
        object_manager_config.huge_pages = huge_pages;

        int num_cpus = static_cast<int>(static_resource_conf["CPU"]);
        object_manager_config.rpc_service_threads_number =
            std::min(std::max(2, num_cpus / 4), 8);
        object_manager_config.object_chunk_size =
            RayConfig::instance().object_manager_default_chunk_size();

        RAY_LOG(DEBUG) << "Starting object manager with configuration: \n"
                       << "rpc_service_threads_number = "
                       << object_manager_config.rpc_service_threads_number
                       << ", object_chunk_size = "
                       << object_manager_config.object_chunk_size;
        // Initialize stats.
        const ray::stats::TagsType global_tags = {
            {ray::stats::ComponentKey, "raylet"},
            {ray::stats::VersionKey, "0.9.0.dev0"},
            {ray::stats::NodeAddressKey, node_ip_address}};
        ray::stats::Init(global_tags, metrics_agent_port);

        // Initialize the node manager.
        server.reset(new ray::raylet::Raylet(
            main_service, raylet_socket_name, node_ip_address, redis_address, redis_port,
            redis_password, node_manager_config, object_manager_config, gcs_client));

        server->Start();
      }));

  // Destroy the Raylet on a SIGTERM. The pointer to main_service is
  // guaranteed to be valid since this function will run the event loop
  // instead of returning immediately.
  // We should stop the service and remove the local socket file.
  auto handler = [&main_service, &raylet_socket_name, &server, &gcs_client](
                     const boost::system::error_code &error, int signal_number) {
    RAY_LOG(INFO) << "Raylet received SIGTERM, shutting down...";
    server->Stop();
    gcs_client->Disconnect();
    ray::stats::Shutdown();
    main_service.stop();
    remove(raylet_socket_name.c_str());
  };
  boost::asio::signal_set signals(main_service);
#ifdef _WIN32
  signals.add(SIGBREAK);
#else
  signals.add(SIGTERM);
#endif
  signals.async_wait(handler);

  main_service.run();
}
#endif
