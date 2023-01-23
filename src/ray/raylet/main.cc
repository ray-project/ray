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
#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/id.h"
#include "ray/common/ray_config.h"
#include "ray/common/status.h"
#include "ray/common/task/task_common.h"
#include "ray/gcs/gcs_client/gcs_client.h"
#include "ray/raylet/raylet.h"
#include "ray/stats/stats.h"
#include "ray/util/event.h"

DEFINE_string(raylet_socket_name, "", "The socket name of raylet.");
DEFINE_string(store_socket_name, "", "The socket name of object store.");
DEFINE_int32(object_manager_port, -1, "The port of object manager.");
DEFINE_int32(node_manager_port, -1, "The port of node manager.");
DEFINE_int32(metrics_agent_port, -1, "The port of metrics agent.");
DEFINE_int32(metrics_export_port, 1, "The port at which metrics are exposed.");
DEFINE_string(node_ip_address, "", "The ip address of this node.");
DEFINE_string(gcs_address, "", "The address of the GCS server, including IP and port.");
DEFINE_int32(min_worker_port,
             0,
             "The lowest port that workers' gRPC servers will bind on.");
DEFINE_int32(max_worker_port,
             0,
             "The highest port that workers' gRPC servers will bind on.");
DEFINE_string(worker_port_list,
              "",
              "An explicit list of ports that workers' gRPC servers will bind on.");
DEFINE_int32(num_prestarted_workers,
             0,
             "Number of prestarted default Python workers on raylet startup.");
DEFINE_int32(maximum_startup_concurrency, 1, "Maximum startup concurrency.");
DEFINE_string(static_resource_list, "", "The static resource list of this node.");
DEFINE_string(python_worker_command, "", "Python worker command.");
DEFINE_string(java_worker_command, "", "Java worker command.");
DEFINE_string(agent_command, "", "Dashboard agent command.");
DEFINE_string(cpp_worker_command, "", "CPP worker command.");
DEFINE_string(native_library_path,
              "",
              "The native library path which includes the core libraries.");
DEFINE_string(temp_dir, "", "Temporary directory.");
DEFINE_string(session_dir, "", "The path of this ray session directory.");
DEFINE_string(log_dir, "", "The path of the dir where log files are created.");
DEFINE_string(resource_dir, "", "The path of this ray resource directory.");
DEFINE_int32(ray_debugger_external, 0, "Make Ray debugger externally accessible.");
// store options
DEFINE_int64(object_store_memory, -1, "The initial memory of the object store.");
DEFINE_string(node_name, "", "The user-provided identifier or name for this node.");
DEFINE_string(session_name, "", "Session name (ClusterID) of the cluster.");
#ifdef __linux__
DEFINE_string(plasma_directory,
              "/dev/shm",
              "The shared memory directory of the object store.");
#else
DEFINE_string(plasma_directory,
              "/tmp",
              "The shared memory directory of the object store.");
#endif
DEFINE_bool(huge_pages, false, "Enable huge pages.");
#ifndef RAYLET_TEST

int main(int argc, char *argv[]) {
  InitShutdownRAII ray_log_shutdown_raii(ray::RayLog::StartRayLog,
                                         ray::RayLog::ShutDownRayLog,
                                         argv[0],
                                         ray::RayLogLevel::INFO,
                                         /*log_dir=*/"");
  ray::RayLog::InstallFailureSignalHandler(argv[0]);
  ray::RayLog::InstallTerminateHandler();

  gflags::ParseCommandLineFlags(&argc, &argv, true);
  const std::string raylet_socket_name = FLAGS_raylet_socket_name;
  const std::string store_socket_name = FLAGS_store_socket_name;
  const std::string node_name =
      (FLAGS_node_name == "") ? FLAGS_node_ip_address : FLAGS_node_name;
  const int object_manager_port = static_cast<int>(FLAGS_object_manager_port);
  const int node_manager_port = static_cast<int>(FLAGS_node_manager_port);
  const int metrics_agent_port = static_cast<int>(FLAGS_metrics_agent_port);
  const std::string node_ip_address = FLAGS_node_ip_address;
  const int min_worker_port = static_cast<int>(FLAGS_min_worker_port);
  const int max_worker_port = static_cast<int>(FLAGS_max_worker_port);
  const std::string worker_port_list = FLAGS_worker_port_list;
  const int num_prestarted_workers = static_cast<int>(FLAGS_num_prestarted_workers);
  const int maximum_startup_concurrency =
      static_cast<int>(FLAGS_maximum_startup_concurrency);
  const std::string static_resource_list = FLAGS_static_resource_list;
  const std::string python_worker_command = FLAGS_python_worker_command;
  const std::string java_worker_command = FLAGS_java_worker_command;
  const std::string agent_command = FLAGS_agent_command;
  const std::string cpp_worker_command = FLAGS_cpp_worker_command;
  const std::string native_library_path = FLAGS_native_library_path;
  const std::string temp_dir = FLAGS_temp_dir;
  const std::string session_dir = FLAGS_session_dir;
  const std::string log_dir = FLAGS_log_dir;
  const std::string resource_dir = FLAGS_resource_dir;
  const int ray_debugger_external = FLAGS_ray_debugger_external;
  const int64_t object_store_memory = FLAGS_object_store_memory;
  const std::string plasma_directory = FLAGS_plasma_directory;
  const bool huge_pages = FLAGS_huge_pages;
  const int metrics_export_port = FLAGS_metrics_export_port;
  const std::string session_name = FLAGS_session_name;
  gflags::ShutDownCommandLineFlags();

  // Configuration for the node manager.
  ray::raylet::NodeManagerConfig node_manager_config;
  absl::flat_hash_map<std::string, double> static_resource_conf;

  // IO Service for node manager.
  instrumented_io_context main_service;

  // Ensure that the IO service keeps running. Without this, the service will exit as soon
  // as there is no more work to be processed.
  boost::asio::io_service::work main_work(main_service);

  // Initialize gcs client
  std::shared_ptr<ray::gcs::GcsClient> gcs_client;
  ray::gcs::GcsClientOptions client_options(FLAGS_gcs_address);
  gcs_client = std::make_shared<ray::gcs::GcsClient>(client_options);

  RAY_CHECK_OK(gcs_client->Connect(main_service));
  std::unique_ptr<ray::raylet::Raylet> raylet;

  RAY_CHECK_OK(gcs_client->Nodes().AsyncGetInternalConfig(
      [&](::ray::Status status,
          const boost::optional<std::string> &stored_raylet_config) {
        RAY_CHECK_OK(status);
        RAY_CHECK(stored_raylet_config.has_value());
        RayConfig::instance().initialize(stored_raylet_config.get());

        // Parse the worker port list.
        std::istringstream worker_port_list_string(worker_port_list);
        std::string worker_port;
        std::vector<int> worker_ports;

        while (std::getline(worker_port_list_string, worker_port, ',')) {
          worker_ports.push_back(std::stoi(worker_port));
        }

        // Parse the resource list.
        std::istringstream resource_string(static_resource_list);
        std::string resource_name;
        std::string resource_quantity;

        while (std::getline(resource_string, resource_name, ',')) {
          RAY_CHECK(std::getline(resource_string, resource_quantity, ','));
          static_resource_conf[resource_name] = std::stod(resource_quantity);
        }
        auto num_cpus_it = static_resource_conf.find("CPU");
        int num_cpus = num_cpus_it != static_resource_conf.end()
                           ? static_cast<int>(num_cpus_it->second)
                           : 0;

        node_manager_config.raylet_config = stored_raylet_config.get();
        node_manager_config.resource_config =
            ray::ResourceMapToResourceRequest(std::move(static_resource_conf), false);
        RAY_LOG(DEBUG) << "Starting raylet with static resource configuration: "
                       << node_manager_config.resource_config.DebugString();
        node_manager_config.node_manager_address = node_ip_address;
        node_manager_config.node_manager_port = node_manager_port;
        auto soft_limit_config = RayConfig::instance().num_workers_soft_limit();
        node_manager_config.num_workers_soft_limit =
            soft_limit_config >= 0 ? soft_limit_config : num_cpus;
        node_manager_config.num_prestarted_workers = num_prestarted_workers;
        node_manager_config.maximum_startup_concurrency = maximum_startup_concurrency;
        node_manager_config.min_worker_port = min_worker_port;
        node_manager_config.max_worker_port = max_worker_port;
        node_manager_config.worker_ports = worker_ports;

        if (!python_worker_command.empty()) {
          node_manager_config.worker_commands.emplace(
              make_pair(ray::Language::PYTHON, ParseCommandLine(python_worker_command)));
        }
        if (!java_worker_command.empty()) {
          node_manager_config.worker_commands.emplace(
              make_pair(ray::Language::JAVA, ParseCommandLine(java_worker_command)));
        }
        if (!cpp_worker_command.empty()) {
          node_manager_config.worker_commands.emplace(
              make_pair(ray::Language::CPP, ParseCommandLine(cpp_worker_command)));
        }
        node_manager_config.native_library_path = native_library_path;
        if (python_worker_command.empty() && java_worker_command.empty() &&
            cpp_worker_command.empty()) {
          RAY_LOG(FATAL) << "At least one of Python/Java/CPP worker command "
                         << "should be provided";
        }
        if (!agent_command.empty()) {
          node_manager_config.agent_command = agent_command;
        } else {
          RAY_LOG(DEBUG) << "Agent command is empty. Not starting agent.";
        }

        node_manager_config.report_resources_period_ms =
            RayConfig::instance().raylet_report_resources_period_milliseconds();
        node_manager_config.record_metrics_period_ms =
            RayConfig::instance().metrics_report_interval_ms() / 2;
        node_manager_config.store_socket_name = store_socket_name;
        node_manager_config.temp_dir = temp_dir;
        node_manager_config.log_dir = log_dir;
        node_manager_config.session_dir = session_dir;
        node_manager_config.resource_dir = resource_dir;
        node_manager_config.ray_debugger_external = ray_debugger_external;
        node_manager_config.max_io_workers = RayConfig::instance().max_io_workers();
        node_manager_config.min_spilling_size = RayConfig::instance().min_spilling_size();

        // Configuration for the object manager.
        ray::ObjectManagerConfig object_manager_config;
        object_manager_config.object_manager_address = node_ip_address;
        object_manager_config.object_manager_port = object_manager_port;
        object_manager_config.store_socket_name = store_socket_name;

        object_manager_config.timer_freq_ms =
            RayConfig::instance().object_manager_timer_freq_ms();
        object_manager_config.pull_timeout_ms =
            RayConfig::instance().object_manager_pull_timeout_ms();
        object_manager_config.push_timeout_ms =
            RayConfig::instance().object_manager_push_timeout_ms();
        if (object_store_memory <= 0) {
          RAY_LOG(FATAL) << "Object store memory should be set.";
        }
        object_manager_config.object_store_memory = object_store_memory;
        object_manager_config.max_bytes_in_flight =
            RayConfig::instance().object_manager_max_bytes_in_flight();
        object_manager_config.plasma_directory = plasma_directory;
        object_manager_config.fallback_directory = temp_dir;
        object_manager_config.huge_pages = huge_pages;

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
            {ray::stats::WorkerIdKey, ""},
            {ray::stats::VersionKey, kRayVersion},
            {ray::stats::NodeAddressKey, node_ip_address},
            {ray::stats::SessionNameKey, session_name}};
        ray::stats::Init(global_tags, metrics_agent_port, WorkerID::Nil());

        // Initialize the node manager.
        raylet = std::make_unique<ray::raylet::Raylet>(main_service,
                                                       raylet_socket_name,
                                                       node_ip_address,
                                                       node_name,
                                                       node_manager_config,
                                                       object_manager_config,
                                                       gcs_client,
                                                       metrics_export_port);

        // Initialize event framework.
        if (RayConfig::instance().event_log_reporter_enabled() && !log_dir.empty()) {
          ray::RayEventInit(ray::rpc::Event_SourceType::Event_SourceType_RAYLET,
                            {{"node_id", raylet->GetNodeId().Hex()}},
                            log_dir,
                            RayConfig::instance().event_level());
        };

        raylet->Start();
      }));

  auto shutted_down = std::make_shared<std::atomic<bool>>(false);

  // Destroy the Raylet on a SIGTERM. The pointer to main_service is
  // guaranteed to be valid since this function will run the event loop
  // instead of returning immediately.
  // We should stop the service and remove the local socket file.
  auto handler = [&main_service, &raylet_socket_name, &raylet, &gcs_client, shutted_down](
                     const boost::system::error_code &error, int signal_number) {
    // Make the shutdown handler idempotent since graceful shutdown can be triggered
    // by many places.
    if (*shutted_down) {
      RAY_LOG(INFO) << "Raylet already received SIGTERM. It will ignore the request.";
      return;
    }
    RAY_LOG(INFO) << "Raylet received SIGTERM, shutting down...";
    *shutted_down = true;
    raylet->Stop();
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
