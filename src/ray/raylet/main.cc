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

#include <algorithm>
#include <cstdlib>
#include <iostream>
#include <limits>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "gflags/gflags.h"
#include "nlohmann/json.hpp"
#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/cgroup/cgroup_manager.h"
#include "ray/common/id.h"
#include "ray/common/ray_config.h"
#include "ray/common/status.h"
#include "ray/common/task/task_common.h"
#include "ray/gcs/gcs_client/gcs_client.h"
#include "ray/object_manager/ownership_object_directory.h"
#include "ray/raylet/raylet.h"
#include "ray/stats/stats.h"
#include "ray/util/cmd_line_utils.h"
#include "ray/util/event.h"
#include "ray/util/process.h"
#include "ray/util/stream_redirection.h"
#include "ray/util/stream_redirection_options.h"
#include "ray/util/subreaper.h"
#include "scheduling/cluster_task_manager.h"
#include "src/ray/protobuf/gcs.pb.h"

using json = nlohmann::json;

DEFINE_string(raylet_socket_name, "", "The socket name of raylet.");
DEFINE_string(store_socket_name, "", "The socket name of object store.");
DEFINE_int32(object_manager_port, -1, "The port of object manager.");
DEFINE_int32(node_manager_port, -1, "The port of node manager.");
DEFINE_int32(metrics_agent_port, -1, "The port of metrics agent.");
DEFINE_int32(metrics_export_port, 1, "The port at which metrics are exposed.");
DEFINE_int32(runtime_env_agent_port, 1, "The port of runtime env agent.");
DEFINE_string(node_id, "", "The id of this node.");
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
DEFINE_int32(num_prestart_python_workers,
             0,
             "Number of prestarted default Python workers on raylet startup.");
DEFINE_bool(head, false, "Whether this node is a head node.");
/// NOTE: This value is overwritten inside worker_pool.h by
/// worker_maximum_startup_concurrency.
DEFINE_int32(maximum_startup_concurrency, 1, "Maximum startup concurrency.");
DEFINE_string(static_resource_list, "", "The static resource list of this node.");
DEFINE_string(python_worker_command, "", "Python worker command.");
DEFINE_string(java_worker_command, "", "Java worker command.");
DEFINE_string(dashboard_agent_command, "", "Dashboard agent command.");
DEFINE_string(runtime_env_agent_command, "", "Runtime env agent command.");
DEFINE_string(cpp_worker_command, "", "CPP worker command.");
DEFINE_string(native_library_path,
              "",
              "The native library path which includes the core libraries.");
DEFINE_string(temp_dir, "", "Temporary directory.");
DEFINE_string(session_dir, "", "The path of this ray session directory.");
DEFINE_string(log_dir, "", "The path of the dir where log files are created.");
DEFINE_string(stdout_filepath, "", "The filepath to dump raylet stdout.");
DEFINE_string(stderr_filepath, "", "The filepath to dump raylet stderr.");
DEFINE_string(resource_dir, "", "The path of this ray resource directory.");
DEFINE_int32(ray_debugger_external, 0, "Make Ray debugger externally accessible.");
// store options
DEFINE_int64(object_store_memory, -1, "The initial memory of the object store.");
DEFINE_string(node_name, "", "The user-provided identifier or name for this node.");
DEFINE_string(session_name, "", "Session name (ClusterID) of the cluster.");
DEFINE_string(cluster_id, "", "ID of the cluster, separate from observability.");
// TODO(hjiang): At the moment only enablement flag is added, I will add other flags for
// CPU and memory resource reservation in the followup PR.
DEFINE_bool(enable_resource_isolation,
            false,
            "Enable resource isolation through cgroupv2 by reserving resources for ray "
            "system processes.");

#ifdef __linux__
DEFINE_string(plasma_directory,
              "/dev/shm",
              "The shared memory directory of the object store.");
#else
DEFINE_string(plasma_directory,
              "/tmp",
              "The shared memory directory of the object store.");
#endif
DEFINE_string(fallback_directory, "", "The directory for fallback allocation files.");
DEFINE_bool(huge_pages, false, "Enable huge pages.");
DEFINE_string(labels,
              "",
              "Define the key-value format of node labels, which is a serialized JSON.");

absl::flat_hash_map<std::string, std::string> parse_node_labels(
    const std::string &labels_json_str) {
  absl::flat_hash_map<std::string, std::string> labels;
  if (labels_json_str.empty()) {
    return labels;
  }
  try {
    json j = json::parse(labels_json_str);
    for (auto &el : j.items()) {
      if (el.value().is_string()) {
        labels.emplace(el.key(), el.value());
      } else {
        throw std::invalid_argument(
            "The value of the '" + el.key() +
            "' field in the node labels configuration is not string type.");
      }
    }
  } catch (const std::exception &e) {
    RAY_LOG(FATAL) << "Failed to parse node labels json string for " << e.what();
  }
  return labels;
}

int main(int argc, char *argv[]) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  if (!FLAGS_stdout_filepath.empty()) {
    ray::StreamRedirectionOption stdout_redirection_options;
    stdout_redirection_options.file_path = FLAGS_stdout_filepath;
    stdout_redirection_options.rotation_max_size =
        ray::RayLog::GetRayLogRotationMaxBytesOrDefault();
    stdout_redirection_options.rotation_max_file_count =
        ray::RayLog::GetRayLogRotationBackupCountOrDefault();
    ray::RedirectStdoutOncePerProcess(stdout_redirection_options);
  }

  if (!FLAGS_stderr_filepath.empty()) {
    ray::StreamRedirectionOption stderr_redirection_options;
    stderr_redirection_options.file_path = FLAGS_stderr_filepath;
    stderr_redirection_options.rotation_max_size =
        ray::RayLog::GetRayLogRotationMaxBytesOrDefault();
    stderr_redirection_options.rotation_max_file_count =
        ray::RayLog::GetRayLogRotationBackupCountOrDefault();
    ray::RedirectStderrOncePerProcess(stderr_redirection_options);
  }

  // Backward compatibility notes:
  // By default, raylet flushes all logging and stdout to a single file called
  // `raylet.out`, without log rotations. To keep backward compatibility at best
  // effort, we use the same filename as output, and disable log rotation by default.
  InitShutdownRAII ray_log_shutdown_raii(ray::RayLog::StartRayLog,
                                         ray::RayLog::ShutDownRayLog,
                                         /*app_name=*/argv[0],
                                         ray::RayLogLevel::INFO,
                                         /*log_filepath=*/"",
                                         /*err_log_filepath=*/"",
                                         /*log_rotation_max_size=*/0,
                                         /*log_rotation_file_num=*/1);

  ray::RayLog::InstallFailureSignalHandler(argv[0]);
  ray::RayLog::InstallTerminateHandler();

#ifdef __linux__
  // Reset LD_PRELOAD if it's loaded with ray jemalloc
  auto ray_ld_preload = std::getenv("RAY_LD_PRELOAD_ON_WORKERS");
  if (ray_ld_preload != nullptr && std::string(ray_ld_preload) == "0") {
    unsetenv("LD_PRELOAD");
  }
#endif
  const std::string raylet_socket_name = FLAGS_raylet_socket_name;
  const std::string store_socket_name = FLAGS_store_socket_name;
  const std::string node_name =
      (FLAGS_node_name == "") ? FLAGS_node_ip_address : FLAGS_node_name;
  const int object_manager_port = static_cast<int>(FLAGS_object_manager_port);
  const int node_manager_port = static_cast<int>(FLAGS_node_manager_port);
  const int metrics_agent_port = static_cast<int>(FLAGS_metrics_agent_port);
  const int runtime_env_agent_port = static_cast<int>(FLAGS_runtime_env_agent_port);
  RAY_CHECK_NE(FLAGS_node_id, "") << "Expected node ID.";
  const std::string node_id = FLAGS_node_id;
  const std::string node_ip_address = FLAGS_node_ip_address;
  const int min_worker_port = static_cast<int>(FLAGS_min_worker_port);
  const int max_worker_port = static_cast<int>(FLAGS_max_worker_port);
  const std::string worker_port_list = FLAGS_worker_port_list;
  const int num_prestart_python_workers =
      static_cast<int>(FLAGS_num_prestart_python_workers);
  const int maximum_startup_concurrency =
      static_cast<int>(FLAGS_maximum_startup_concurrency);
  const std::string static_resource_list = FLAGS_static_resource_list;
  const std::string python_worker_command = FLAGS_python_worker_command;
  const std::string java_worker_command = FLAGS_java_worker_command;
  const std::string dashboard_agent_command = FLAGS_dashboard_agent_command;
  const std::string runtime_env_agent_command = FLAGS_runtime_env_agent_command;
  const std::string cpp_worker_command = FLAGS_cpp_worker_command;
  const std::string native_library_path = FLAGS_native_library_path;
  const std::string session_dir = FLAGS_session_dir;
  const std::string log_dir = FLAGS_log_dir;
  const std::string resource_dir = FLAGS_resource_dir;
  const int ray_debugger_external = FLAGS_ray_debugger_external;
  const int64_t object_store_memory = FLAGS_object_store_memory;
  const std::string plasma_directory = FLAGS_plasma_directory;
  const std::string fallback_directory = FLAGS_fallback_directory;
  const bool huge_pages = FLAGS_huge_pages;
  const int metrics_export_port = FLAGS_metrics_export_port;
  const std::string session_name = FLAGS_session_name;
  const bool is_head_node = FLAGS_head;
  const std::string labels_json_str = FLAGS_labels;

  RAY_CHECK_NE(FLAGS_cluster_id, "") << "Expected cluster ID.";
  ray::ClusterID cluster_id = ray::ClusterID::FromHex(FLAGS_cluster_id);
  RAY_LOG(INFO) << "Setting cluster ID to: " << cluster_id;
  gflags::ShutDownCommandLineFlags();

  // Get cgroup setup instance and perform necessary resource setup.
  ray::GetCgroupSetup(FLAGS_enable_resource_isolation);

  // Configuration for the node manager.
  ray::raylet::NodeManagerConfig node_manager_config;
  node_manager_config.enable_resource_isolation = FLAGS_enable_resource_isolation;

  absl::flat_hash_map<std::string, double> static_resource_conf;

  SetThreadName("raylet");
  // IO Service for node manager.
  instrumented_io_context main_service{/*enable_lag_probe=*/false,
                                       /*running_on_single_thread=*/true};

  // Ensure that the IO service keeps running. Without this, the service will exit as soon
  // as there is no more work to be processed.
  boost::asio::executor_work_guard<boost::asio::io_context::executor_type>
      main_service_work(main_service.get_executor());

  // Initialize gcs client
  std::shared_ptr<ray::gcs::GcsClient> gcs_client;
  ray::gcs::GcsClientOptions client_options(FLAGS_gcs_address,
                                            cluster_id,
                                            /*allow_cluster_id_nil=*/false,
                                            /*fetch_cluster_id_if_nil=*/false);
  gcs_client = std::make_shared<ray::gcs::GcsClient>(client_options);

  RAY_CHECK_OK(gcs_client->Connect(main_service));
  std::unique_ptr<ray::raylet::Raylet> raylet;

  std::shared_ptr<plasma::PlasmaClient> plasma_client;
  std::shared_ptr<ray::raylet::NodeManager> node_manager;
  std::shared_ptr<ray::rpc::ClientCallManager> client_call_manager;
  std::shared_ptr<ray::rpc::CoreWorkerClientPool> worker_rpc_pool;
  std::shared_ptr<ray::raylet::WorkerPoolInterface> worker_pool;
  /// Manages all local objects that are pinned (primary
  /// copies), freed, and/or spilled.
  std::shared_ptr<ray::raylet::LocalObjectManager> local_object_manager;
  /// These classes make up the new scheduler. ClusterResourceScheduler is
  /// responsible for maintaining a view of the cluster state w.r.t resource
  /// usage. ClusterTaskManager is responsible for queuing, spilling back, and
  /// dispatching tasks.
  std::shared_ptr<ray::ClusterResourceScheduler> cluster_resource_scheduler;
  std::shared_ptr<ray::raylet::ILocalTaskManager> local_task_manager;
  std::shared_ptr<ray::raylet::ClusterTaskManagerInterface> cluster_task_manager;
  /// The raylet client to initiate the pubsub to core workers (owners).
  /// It is used to subscribe objects to evict.
  std::shared_ptr<ray::pubsub::SubscriberInterface> core_worker_subscriber;
  /// The object table. This is shared between the object manager and node
  /// manager.
  std::shared_ptr<ray::IObjectDirectory> object_directory;
  /// Manages client requests for object transfers and availability.
  std::shared_ptr<ray::ObjectManagerInterface> object_manager;
  /// A manager to resolve objects needed by queued tasks and workers that
  /// called `ray.get` or `ray.wait`.
  std::shared_ptr<ray::raylet::DependencyManager> dependency_manager;
  /// Map of workers leased out to clients.
  absl::flat_hash_map<WorkerID, std::shared_ptr<ray::raylet::WorkerInterface>>
      leased_workers;

  // Enable subreaper. This is called in `AsyncGetInternalConfig` below, but MSVC does
  // not allow a macro invocation (#ifdef) in another macro invocation (RAY_CHECK_OK),
  // so we have to put it here.
  auto enable_subreaper = [&]() {
#ifdef __linux__
    if (ray::SetThisProcessAsSubreaper()) {
      ray::KnownChildrenTracker::instance().Enable();
      ray::SetupSigchldHandlerRemoveKnownChildren(main_service);
      auto runner = ray::PeriodicalRunner::Create(main_service);
      runner->RunFnPeriodically([runner]() { ray::KillUnknownChildren(); },
                                /*period_ms=*/10000,
                                "Raylet.KillUnknownChildren");
      RAY_LOG(INFO) << "Set this process as subreaper. Will kill unknown children every "
                       "10 seconds.";
    } else {
      RAY_LOG(WARNING) << "Failed to set this process as subreaper. Will not kill "
                          "unknown children.";
      ray::SetSigchldIgnore();
    }
#else
    RAY_LOG(WARNING) << "Subreaper is not supported on this platform. Will not "
                        "kill unknown children.";
    ray::SetSigchldIgnore();
#endif
  };

  auto shutted_down = std::make_shared<std::atomic<bool>>(false);

  auto shutdown_raylet_after_unregistration =
      [&main_service, &raylet_socket_name, &raylet, &gcs_client]() {
        // We should stop the service and remove the local socket file.
        raylet->Stop();
        gcs_client->Disconnect();
        ray::stats::Shutdown();
        main_service.stop();
        remove(raylet_socket_name.c_str());
      };

  // Shut down raylet gracefully, in a synchronous fashion.
  // This is an internal method and should only be run on the main_service.
  auto shutdown_raylet_gracefully_internal =
      [&raylet, shutted_down, shutdown_raylet_after_unregistration](
          const ray::rpc::NodeDeathInfo &node_death_info) {
        // Make the shutdown method idempotent since graceful shutdown can be triggered
        // by many places.
        if (*shutted_down) {
          RAY_LOG(INFO) << "Raylet shutdown already triggered, ignoring this request.";
          return;
        }
        RAY_LOG(INFO) << "Raylet graceful shutdown triggered, reason = "
                      << NodeDeathInfo_Reason_Name(node_death_info.reason()) << ", "
                      << "reason message = " << node_death_info.reason_message();
        RAY_LOG(INFO) << "Shutting down...";
        *shutted_down = true;

        raylet->UnregisterSelf(node_death_info, shutdown_raylet_after_unregistration);
      };

  auto shutdown_raylet_gracefully = [&main_service, shutdown_raylet_gracefully_internal](
                                        const ray::rpc::NodeDeathInfo &node_death_info) {
    main_service.post(
        [shutdown_raylet_gracefully_internal, node_death_info]() {
          shutdown_raylet_gracefully_internal(node_death_info);
        },
        "shutdown_raylet_gracefully_internal");
  };

  ray::NodeID raylet_node_id = ray::NodeID::FromHex(node_id);

  RAY_CHECK_OK(gcs_client->InternalKV().AsyncGetInternalConfig(
      [&](::ray::Status status, const std::optional<std::string> &stored_raylet_config) {
        RAY_CHECK_OK(status);
        RAY_CHECK(stored_raylet_config.has_value());
        RayConfig::instance().initialize(*stored_raylet_config);
        ray::asio::testing::Init();
        ray::rpc::testing::Init();

        // Core worker tries to kill child processes when it exits. But they can't do
        // it perfectly: if the core worker is killed by SIGKILL, the child processes
        // leak. So in raylet we also kill child processes via Linux subreaper.
        // Only works on Linux >= 3.4.
        if (RayConfig::instance()
                .kill_child_processes_on_worker_exit_with_raylet_subreaper()) {
          enable_subreaper();
        } else {
          RAY_LOG(INFO) << "Raylet is not set to kill unknown children.";
          ray::SetSigchldIgnore();
        }

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

        node_manager_config.raylet_config = *stored_raylet_config;
        node_manager_config.resource_config = ray::ResourceSet(static_resource_conf);
        RAY_LOG(DEBUG) << "Starting raylet with static resource configuration: "
                       << node_manager_config.resource_config.DebugString();
        node_manager_config.node_manager_address = node_ip_address;
        node_manager_config.node_manager_port = node_manager_port;
        node_manager_config.num_workers_soft_limit =
            RayConfig::instance().num_workers_soft_limit();
        node_manager_config.num_prestart_python_workers = num_prestart_python_workers;
        node_manager_config.maximum_startup_concurrency = maximum_startup_concurrency;
        node_manager_config.runtime_env_agent_port = runtime_env_agent_port;
        node_manager_config.min_worker_port = min_worker_port;
        node_manager_config.max_worker_port = max_worker_port;
        node_manager_config.worker_ports = worker_ports;
        node_manager_config.labels = parse_node_labels(labels_json_str);

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
        if (dashboard_agent_command.empty()) {
          RAY_LOG(FATAL) << "Dashboard agent command must be non empty";
        }
        node_manager_config.dashboard_agent_command = dashboard_agent_command;

        if (runtime_env_agent_command.empty()) {
          RAY_LOG(FATAL) << "Runtime env agent command must be non empty";
        }
        node_manager_config.runtime_env_agent_command = runtime_env_agent_command;

        node_manager_config.report_resources_period_ms =
            RayConfig::instance().raylet_report_resources_period_milliseconds();
        node_manager_config.record_metrics_period_ms =
            RayConfig::instance().metrics_report_interval_ms() / 2;
        node_manager_config.store_socket_name = store_socket_name;
        node_manager_config.log_dir = log_dir;
        node_manager_config.session_dir = session_dir;
        node_manager_config.resource_dir = resource_dir;
        node_manager_config.ray_debugger_external = ray_debugger_external;
        node_manager_config.max_io_workers = RayConfig::instance().max_io_workers();

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
        object_manager_config.fallback_directory = fallback_directory;
        object_manager_config.huge_pages = huge_pages;

        object_manager_config.rpc_service_threads_number =
            std::min(std::max(2, num_cpus / 4), 8);
        if (RayConfig::instance().object_manager_rpc_threads_num() != 0) {
          object_manager_config.rpc_service_threads_number =
              RayConfig::instance().object_manager_rpc_threads_num();
        }
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

        RAY_LOG(INFO).WithField(raylet_node_id) << "Setting node ID";

        node_manager_config.AddDefaultLabels(raylet_node_id.Hex());

        worker_pool = std::make_shared<ray::raylet::WorkerPool>(
            main_service,
            raylet_node_id,
            node_manager_config.node_manager_address,
            [&]() {
              // Callback to determine the maximum number of idle workers to
              // keep around.
              if (node_manager_config.num_workers_soft_limit >= 0) {
                return node_manager_config.num_workers_soft_limit;
              }
              // If no limit is provided, use the available number of CPUs,
              // assuming that each incoming task will likely require 1 CPU.
              // We floor the available CPUs to the nearest integer to avoid
              // starting too many workers when there is less than 1 CPU left.
              // Otherwise, we could end up repeatedly starting the worker, then
              // killing it because it idles for too long. The downside is that
              // we will be slower to schedule tasks that could use a fraction
              // of a CPU.
              return static_cast<int64_t>(
                  cluster_resource_scheduler->GetLocalResourceManager()
                      .GetLocalAvailableCpus());
            },
            node_manager_config.num_prestart_python_workers,
            node_manager_config.maximum_startup_concurrency,
            node_manager_config.min_worker_port,
            node_manager_config.max_worker_port,
            node_manager_config.worker_ports,
            gcs_client,
            node_manager_config.worker_commands,
            node_manager_config.native_library_path,
            /*starting_worker_timeout_callback=*/
            [&] { cluster_task_manager->ScheduleAndDispatchTasks(); },
            node_manager_config.ray_debugger_external,
            /*get_time=*/[]() { return absl::Now(); },
            node_manager_config.enable_resource_isolation);

        client_call_manager = std::make_shared<ray::rpc::ClientCallManager>(
            main_service, /*record_stats=*/true);

        worker_rpc_pool = std::make_shared<ray::rpc::CoreWorkerClientPool>(
            [&](const ray::rpc::Address &addr) {
              return std::make_shared<ray::rpc::CoreWorkerClient>(
                  addr,
                  *client_call_manager,
                  ray::rpc::CoreWorkerClientPool::GetDefaultUnavailableTimeoutCallback(
                      gcs_client.get(),
                      worker_rpc_pool.get(),
                      [&](const std::string &node_manager_address, int32_t port) {
                        return std::make_shared<ray::raylet::RayletClient>(
                            ray::rpc::NodeManagerWorkerClient::make(
                                node_manager_address, port, *client_call_manager));
                      },
                      addr));
            });

        core_worker_subscriber = std::make_shared<ray::pubsub::Subscriber>(
            raylet_node_id,
            /*channels=*/
            std::vector<ray::rpc::ChannelType>{
                ray::rpc::ChannelType::WORKER_OBJECT_EVICTION,
                ray::rpc::ChannelType::WORKER_REF_REMOVED_CHANNEL,
                ray::rpc::ChannelType::WORKER_OBJECT_LOCATIONS_CHANNEL},
            RayConfig::instance().max_command_batch_size(),
            /*get_client=*/
            [&](const ray::rpc::Address &address) {
              return worker_rpc_pool->GetOrConnect(address);
            },
            &main_service);

        object_directory = std::make_shared<ray::OwnershipBasedObjectDirectory>(
            main_service,
            gcs_client,
            core_worker_subscriber.get(),
            worker_rpc_pool.get(),
            [&](const ObjectID &obj_id, const ray::rpc::ErrorType &error_type) {
              ray::rpc::ObjectReference ref;
              ref.set_object_id(obj_id.Binary());
              node_manager->MarkObjectsAsFailed(error_type, {ref}, JobID::Nil());
            });

        object_manager = std::make_shared<ray::ObjectManager>(
            main_service,
            raylet_node_id,
            object_manager_config,
            gcs_client,
            object_directory.get(),
            /*restore_spilled_object=*/
            [&](const ObjectID &object_id,
                int64_t object_size,
                const std::string &object_url,
                std::function<void(const ray::Status &)> callback) {
              local_object_manager->AsyncRestoreSpilledObject(
                  object_id, object_size, object_url, std::move(callback));
            },
            /*get_spilled_object_url=*/
            [&](const ObjectID &object_id) {
              return local_object_manager->GetLocalSpilledObjectURL(object_id);
            },
            /*spill_objects_callback=*/
            [&]() {
              // This callback is called from the plasma store thread.
              // NOTE: It means the local object manager should be thread-safe.
              main_service.post(
                  [&]() { local_object_manager->SpillObjectUptoMaxThroughput(); },
                  "NodeManager.SpillObjects");
              return local_object_manager->IsSpillingInProgress();
            },
            /*object_store_full_callback=*/
            [&]() {
              // Post on the node manager's event loop since this
              // callback is called from the plasma store thread.
              // This will help keep node manager lock-less.
              main_service.post([&]() { node_manager->TriggerGlobalGC(); },
                                "NodeManager.GlobalGC");
            },
            /*add_object_callback=*/
            [&](const ray::ObjectInfo &object_info) {
              node_manager->HandleObjectLocal(object_info);
            },
            /*delete_object_callback=*/
            [&](const ObjectID &object_id) {
              node_manager->HandleObjectMissing(object_id);
            },
            /*pin_object=*/
            [&](const ObjectID &object_id) {
              std::vector<ObjectID> object_ids = {object_id};
              std::vector<std::unique_ptr<ray::RayObject>> results;
              std::unique_ptr<ray::RayObject> result;
              if (node_manager->GetObjectsFromPlasma(object_ids, &results) &&
                  results.size() > 0) {
                result = std::move(results[0]);
              }
              return result;
            },
            /*fail_pull_request=*/
            [&](const ObjectID &object_id, ray::rpc::ErrorType error_type) {
              ray::rpc::ObjectReference ref;
              ref.set_object_id(object_id.Binary());
              node_manager->MarkObjectsAsFailed(error_type, {ref}, JobID::Nil());
            });

        local_object_manager = std::make_shared<ray::raylet::LocalObjectManager>(
            raylet_node_id,
            node_manager_config.node_manager_address,
            node_manager_config.node_manager_port,
            main_service,
            RayConfig::instance().free_objects_batch_size(),
            RayConfig::instance().free_objects_period_milliseconds(),
            *worker_pool,
            *worker_rpc_pool,
            /*max_io_workers*/ node_manager_config.max_io_workers,
            /*is_external_storage_type_fs*/
            RayConfig::instance().is_external_storage_type_fs(),
            /*max_fused_object_count*/ RayConfig::instance().max_fused_object_count(),
            /*on_objects_freed*/
            [&](const std::vector<ObjectID> &object_ids) {
              object_manager->FreeObjects(object_ids,
                                          /*local_only=*/false);
            },
            /*is_plasma_object_spillable*/
            [&](const ObjectID &object_id) {
              return object_manager->IsPlasmaObjectSpillable(object_id);
            },
            /*core_worker_subscriber_=*/core_worker_subscriber.get(),
            object_directory.get());

        dependency_manager =
            std::make_shared<ray::raylet::DependencyManager>(*object_manager);

        cluster_resource_scheduler = std::make_shared<ray::ClusterResourceScheduler>(
            main_service,
            ray::scheduling::NodeID(raylet_node_id.Binary()),
            node_manager_config.resource_config.GetResourceMap(),
            /*is_node_available_fn*/
            [&](ray::scheduling::NodeID node_id) {
              return gcs_client->Nodes().Get(NodeID::FromBinary(node_id.Binary())) !=
                     nullptr;
            },
            /*get_used_object_store_memory*/
            [&]() {
              if (RayConfig::instance().scheduler_report_pinned_bytes_only()) {
                // Get the current bytes used by local primary object copies.  This
                // is used to help node scale down decisions. A node can only be
                // safely drained when this function reports zero.
                int64_t bytes_used = local_object_manager->GetPrimaryBytes();
                // Report nonzero if we have objects spilled to the local filesystem.
                if (bytes_used == 0 && local_object_manager->HasLocallySpilledObjects()) {
                  bytes_used = 1;
                }
                return bytes_used;
              }
              return object_manager->GetUsedMemory();
            },
            /*get_pull_manager_at_capacity*/
            [&]() { return object_manager->PullManagerHasPullsQueued(); },
            shutdown_raylet_gracefully,
            /*labels*/
            node_manager_config.labels);

        auto get_node_info_func = [&](const NodeID &node_id) {
          return gcs_client->Nodes().Get(node_id);
        };
        auto announce_infeasible_task = [](const ray::RayTask &task) {
          /// Publish the infeasible task error to GCS so that drivers can subscribe to it
          /// and print.
          bool suppress_warning = false;

          if (!task.GetTaskSpecification().PlacementGroupBundleId().first.IsNil()) {
            // If the task is part of a placement group, do nothing. If necessary, the
            // infeasible warning should come from the placement group scheduling, not the
            // task scheduling.
            suppress_warning = true;
          }

          // Push a warning to the task's driver that this task is currently infeasible.
          if (!suppress_warning) {
            std::ostringstream error_message;
            error_message
                << "The actor or task with ID " << task.GetTaskSpecification().TaskId()
                << " cannot be scheduled right now. It requires "
                << task.GetTaskSpecification()
                       .GetRequiredPlacementResources()
                       .DebugString()
                << " for placement, however the cluster currently cannot provide the "
                   "requested "
                   "resources. The required resources may be added as autoscaling takes "
                   "place "
                   "or placement groups are scheduled. Otherwise, consider reducing the "
                   "resource requirements of the task.";
            std::string error_message_str = error_message.str();
            RAY_LOG(WARNING) << error_message_str;
          }
        };

        RAY_CHECK(RayConfig::instance().max_task_args_memory_fraction() > 0 &&
                  RayConfig::instance().max_task_args_memory_fraction() <= 1)
            << "max_task_args_memory_fraction must be a nonzero fraction.";
        auto max_task_args_memory =
            static_cast<int64_t>(static_cast<float>(object_manager->GetMemoryCapacity()) *
                                 RayConfig::instance().max_task_args_memory_fraction());
        if (max_task_args_memory <= 0) {
          RAY_LOG(WARNING)
              << "Max task args should be a fraction of the object store capacity, but "
                 "object "
                 "store capacity is zero or negative. Allowing task args to use 100% of "
                 "the "
                 "local object store. This can cause ObjectStoreFullErrors if the tasks' "
                 "return values are greater than the remaining capacity.";
          max_task_args_memory = 0;
        }

        local_task_manager = std::make_shared<ray::raylet::LocalTaskManager>(
            raylet_node_id,
            *std::dynamic_pointer_cast<ray::ClusterResourceScheduler>(
                cluster_resource_scheduler),
            *dependency_manager,
            get_node_info_func,
            *worker_pool,
            leased_workers,
            [&](const std::vector<ObjectID> &object_ids,
                std::vector<std::unique_ptr<ray::RayObject>> *results) {
              return node_manager->GetObjectsFromPlasma(object_ids, results);
            },
            max_task_args_memory);

        cluster_task_manager = std::make_shared<ray::raylet::ClusterTaskManager>(
            raylet_node_id,
            *std::dynamic_pointer_cast<ray::ClusterResourceScheduler>(
                cluster_resource_scheduler),
            get_node_info_func,
            announce_infeasible_task,
            *local_task_manager);

        auto raylet_client_factory =
            [&](const NodeID &node_id, ray::rpc::ClientCallManager &client_call_manager) {
              const ray::rpc::GcsNodeInfo *node_info = gcs_client->Nodes().Get(node_id);
              RAY_CHECK(node_info) << "No GCS info for node " << node_id;
              std::shared_ptr<ray::rpc::NodeManagerWorkerClient> raylet_client =
                  ray::rpc::NodeManagerWorkerClient::make(
                      node_info->node_manager_address(),
                      node_info->node_manager_port(),
                      client_call_manager);
              return std::make_shared<ray::raylet::RayletClient>(
                  std::move(raylet_client));
            };

        plasma_client = std::make_shared<plasma::PlasmaClient>();
        node_manager = std::make_shared<ray::raylet::NodeManager>(
            main_service,
            raylet_node_id,
            node_name,
            node_manager_config,
            gcs_client,
            *client_call_manager,
            *worker_rpc_pool,
            core_worker_subscriber,
            cluster_resource_scheduler,
            local_task_manager,
            cluster_task_manager,
            object_directory,
            object_manager,
            *local_object_manager,
            *dependency_manager,
            *worker_pool,
            leased_workers,
            *plasma_client,
            std::make_unique<ray::core::experimental::MutableObjectProvider>(
                *plasma_client,
                std::move(raylet_client_factory),
                /*check_signals=*/nullptr),
            shutdown_raylet_gracefully);

        // Initialize the node manager.
        raylet = std::make_unique<ray::raylet::Raylet>(main_service,
                                                       raylet_node_id,
                                                       raylet_socket_name,
                                                       node_ip_address,
                                                       node_name,
                                                       node_manager_config,
                                                       object_manager_config,
                                                       gcs_client,
                                                       metrics_export_port,
                                                       is_head_node,
                                                       node_manager);

        // Initialize event framework.
        if (RayConfig::instance().event_log_reporter_enabled() && !log_dir.empty()) {
          const std::vector<ray::SourceTypeVariant> source_types = {
              ray::rpc::Event_SourceType::Event_SourceType_RAYLET};
          ray::RayEventInit(source_types,
                            {{"node_id", raylet->GetNodeId().Hex()}},
                            log_dir,
                            RayConfig::instance().event_level(),
                            RayConfig::instance().emit_event_to_log_file());
        };

        raylet->Start();
      }));

  auto signal_handler = [&raylet, shutdown_raylet_gracefully_internal](
                            const boost::system::error_code &error, int signal_number) {
    ray::rpc::NodeDeathInfo node_death_info;
    std::optional<ray::rpc::DrainRayletRequest> drain_request =
        raylet->node_manager().GetLocalDrainRequest();
    RAY_LOG(INFO) << "received SIGTERM. Existing local drain request = "
                  << (drain_request.has_value() ? drain_request->DebugString() : "None");
    if (drain_request.has_value() &&
        drain_request->reason() ==
            ray::rpc::autoscaler::DrainNodeReason::DRAIN_NODE_REASON_PREEMPTION &&
        drain_request->deadline_timestamp_ms() != 0 &&
        drain_request->deadline_timestamp_ms() < current_sys_time_ms()) {
      node_death_info.set_reason(ray::rpc::NodeDeathInfo::AUTOSCALER_DRAIN_PREEMPTED);
      node_death_info.set_reason_message(drain_request->reason_message());
    } else {
      node_death_info.set_reason(ray::rpc::NodeDeathInfo::EXPECTED_TERMINATION);
      node_death_info.set_reason_message("received SIGTERM");
    }

    shutdown_raylet_gracefully_internal(node_death_info);
  };
  boost::asio::signal_set signals(main_service);
#ifdef _WIN32
  signals.add(SIGBREAK);
#else
  signals.add(SIGTERM);
#endif
  signals.async_wait(signal_handler);

  main_service.run();
}
