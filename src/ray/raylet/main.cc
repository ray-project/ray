#include <iostream>

#include "ray/common/id.h"
#include "ray/common/ray_config.h"
#include "ray/common/status.h"
#include "ray/common/task/task_common.h"
#include "ray/raylet/raylet.h"
#include "ray/stats/stats.h"

#include "gflags/gflags.h"

DEFINE_string(raylet_socket_name, "", "The socket name of raylet.");
DEFINE_string(store_socket_name, "", "The socket name of object store.");
DEFINE_int32(object_manager_port, -1, "The port of object manager.");
DEFINE_int32(node_manager_port, -1, "The port of node manager.");
DEFINE_string(node_ip_address, "", "The ip address of this node.");
DEFINE_string(redis_address, "", "The ip address of redis server.");
DEFINE_int32(redis_port, -1, "The port of redis server.");
DEFINE_int32(num_initial_workers, 0, "Number of initial workers.");
DEFINE_int32(maximum_startup_concurrency, 1, "Maximum startup concurrency");
DEFINE_string(static_resource_list, "", "The static resource list of this node.");
DEFINE_string(config_list, "", "The raylet config list of this node.");
DEFINE_string(python_worker_command, "", "Python worker command.");
DEFINE_string(java_worker_command, "", "Java worker command.");
DEFINE_string(redis_password, "", "The password of redis.");
DEFINE_string(temp_dir, "", "Temporary directory.");
DEFINE_string(session_dir, "", "The path of this ray session directory.");
DEFINE_bool(disable_stats, false, "Whether disable the stats.");
DEFINE_string(stat_address, "127.0.0.1:8888", "The address that we report metrics to.");
DEFINE_bool(enable_stdout_exporter, false,
            "Whether enable the stdout exporter for stats.");

#ifndef RAYLET_TEST

/// A helper function that parse the worker command string into a vector of arguments.
static std::vector<std::string> parse_worker_command(std::string worker_command) {
  std::istringstream iss(worker_command);
  std::vector<std::string> result(std::istream_iterator<std::string>{iss},
                                  std::istream_iterator<std::string>());
  return result;
}

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
  const std::string node_ip_address = FLAGS_node_ip_address;
  const std::string redis_address = FLAGS_redis_address;
  const int redis_port = static_cast<int>(FLAGS_redis_port);
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
  const bool disable_stats = FLAGS_disable_stats;
  const std::string stat_address = FLAGS_stat_address;
  const bool enable_stdout_exporter = FLAGS_enable_stdout_exporter;
  gflags::ShutDownCommandLineFlags();

  // Initialize stats.
  const ray::stats::TagsType global_tags = {
      {ray::stats::JobNameKey, "raylet"},
      {ray::stats::VersionKey, "0.7.2"},
      {ray::stats::NodeAddressKey, node_ip_address}};
  ray::stats::Init(stat_address, global_tags, disable_stats, enable_stdout_exporter);

  // Configuration for the node manager.
  ray::raylet::NodeManagerConfig node_manager_config;
  std::unordered_map<std::string, double> static_resource_conf;
  std::unordered_map<std::string, std::string> raylet_config;

  // Parse the configuration list.
  std::istringstream config_string(config_list);
  std::string config_name;
  std::string config_value;

  while (std::getline(config_string, config_name, ',')) {
    RAY_CHECK(std::getline(config_string, config_value, ','));
    // TODO(rkn): The line below could throw an exception. What should we do about this?
    raylet_config[config_name] = config_value;
  }

  RayConfig::instance().initialize(raylet_config);

  // Parse the resource list.
  std::istringstream resource_string(static_resource_list);
  std::string resource_name;
  std::string resource_quantity;

  while (std::getline(resource_string, resource_name, ',')) {
    RAY_CHECK(std::getline(resource_string, resource_quantity, ','));
    // TODO(rkn): The line below could throw an exception. What should we do about this?
    static_resource_conf[resource_name] = std::stod(resource_quantity);
  }

  node_manager_config.resource_config = ray::ResourceSet(std::move(static_resource_conf));
  RAY_LOG(DEBUG) << "Starting raylet with static resource configuration: "
                 << node_manager_config.resource_config.ToString();
  node_manager_config.node_manager_address = node_ip_address;
  node_manager_config.node_manager_port = node_manager_port;
  node_manager_config.num_initial_workers = num_initial_workers;
  node_manager_config.num_workers_per_process =
      RayConfig::instance().num_workers_per_process();
  node_manager_config.maximum_startup_concurrency = maximum_startup_concurrency;

  if (!python_worker_command.empty()) {
    node_manager_config.worker_commands.emplace(
        make_pair(ray::Language::PYTHON, parse_worker_command(python_worker_command)));
  }
  if (!java_worker_command.empty()) {
    node_manager_config.worker_commands.emplace(
        make_pair(ray::Language::JAVA, parse_worker_command(java_worker_command)));
  }
  if (python_worker_command.empty() && java_worker_command.empty()) {
    RAY_CHECK(0)
        << "Either Python worker command or Java worker command should be provided.";
  }

  node_manager_config.heartbeat_period_ms =
      RayConfig::instance().heartbeat_timeout_milliseconds();
  node_manager_config.debug_dump_period_ms =
      RayConfig::instance().debug_dump_period_milliseconds();
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

  int num_cpus = static_cast<int>(static_resource_conf["CPU"]);
  object_manager_config.rpc_service_threads_number = std::max(2, num_cpus / 2);
  object_manager_config.object_chunk_size =
      RayConfig::instance().object_manager_default_chunk_size();

  RAY_LOG(DEBUG) << "Starting object manager with configuration: \n"
                 << "rpc_service_threads_number = "
                 << object_manager_config.rpc_service_threads_number
                 << "object_chunk_size = " << object_manager_config.object_chunk_size;

  // Initialize the node manager.
  boost::asio::io_service main_service;

  // Initialize gcs client
  ray::gcs::GcsClientOptions client_options(redis_address, redis_port, redis_password);
  auto gcs_client = std::make_shared<ray::gcs::RedisGcsClient>(client_options);
  RAY_CHECK_OK(gcs_client->Connect(main_service));

  std::unique_ptr<ray::raylet::Raylet> server(new ray::raylet::Raylet(
      main_service, raylet_socket_name, node_ip_address, redis_address, redis_port,
      redis_password, node_manager_config, object_manager_config, gcs_client));

  // Destroy the Raylet on a SIGTERM. The pointer to main_service is
  // guaranteed to be valid since this function will run the event loop
  // instead of returning immediately.
  // We should stop the service and remove the local socket file.
  auto handler = [&main_service, &raylet_socket_name, &server, &gcs_client](
                     const boost::system::error_code &error, int signal_number) {
    auto shutdown_callback = [&server, &main_service, &gcs_client]() {
      server.reset();
      gcs_client->Disconnect();
      main_service.stop();
    };
    RAY_CHECK_OK(gcs_client->client_table().Disconnect(shutdown_callback));
    // Give a timeout for this Disconnect operation.
    boost::posix_time::milliseconds stop_timeout(800);
    boost::asio::deadline_timer timer(main_service);
    timer.expires_from_now(stop_timeout);
    timer.async_wait([shutdown_callback](const boost::system::error_code &error) {
      if (!error) {
        shutdown_callback();
      }
    });
    remove(raylet_socket_name.c_str());
  };
  boost::asio::signal_set signals(main_service, SIGTERM);
  signals.async_wait(handler);

  main_service.run();
}
#endif
