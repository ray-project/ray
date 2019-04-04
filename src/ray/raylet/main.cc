#include <iostream>

#include "ray/ray_config.h"
#include "ray/raylet/raylet.h"
#include "ray/stats/stats.h"
#include "ray/status.h"

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
  RAY_CHECK(argc >= 14 && argc <= 18);

  const std::string raylet_socket_name = std::string(argv[1]);
  const std::string store_socket_name = std::string(argv[2]);
  int object_manager_port = std::stoi(argv[3]);
  int node_manager_port = std::stoi(argv[4]);
  const std::string node_ip_address = std::string(argv[5]);
  const std::string redis_address = std::string(argv[6]);
  int redis_port = std::stoi(argv[7]);
  int num_initial_workers = std::stoi(argv[8]);
  int maximum_startup_concurrency = std::stoi(argv[9]);
  const std::string static_resource_list = std::string(argv[10]);
  const std::string config_list = std::string(argv[11]);
  const std::string python_worker_command = std::string(argv[12]);
  const std::string java_worker_command = std::string(argv[13]);
  const std::string redis_password = (argc >= 15 ? std::string(argv[14]) : "");
  const std::string temp_dir = (argc >= 16 ? std::string(argv[15]) : "/tmp/ray");
  const std::string disable_stats_str(argc >= 17 ? std::string(argv[16]) : "false");
  const bool disable_stats = ("true" == disable_stats_str);
  const std::string stat_address =
      (argc >= 18 ? std::string(argv[17]) : "127.0.0.1:8888");

  // Initialize stats.
  const ray::stats::TagsType global_tags = {
      {ray::stats::JobNameKey, "raylet"},
      {ray::stats::VersionKey, "0.7.0"},
      {ray::stats::NodeAddressKey, node_ip_address}};
  ray::stats::Init(stat_address, global_tags, disable_stats);

  // Configuration for the node manager.
  ray::raylet::NodeManagerConfig node_manager_config;
  std::unordered_map<std::string, double> static_resource_conf;
  std::unordered_map<std::string, int> raylet_config;

  // Parse the configuration list.
  std::istringstream config_string(config_list);
  std::string config_name;
  std::string config_value;

  while (std::getline(config_string, config_name, ',')) {
    RAY_CHECK(std::getline(config_string, config_value, ','));
    // TODO(rkn): The line below could throw an exception. What should we do about this?
    raylet_config[config_name] = std::stoi(config_value);
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

  node_manager_config.resource_config =
      ray::raylet::ResourceSet(std::move(static_resource_conf));
  RAY_LOG(DEBUG) << "Starting raylet with static resource configuration: "
                 << node_manager_config.resource_config.ToString();
  node_manager_config.node_manager_port = node_manager_port;
  node_manager_config.num_initial_workers = num_initial_workers;
  node_manager_config.num_workers_per_process =
      RayConfig::instance().num_workers_per_process();
  node_manager_config.maximum_startup_concurrency = maximum_startup_concurrency;

  if (!python_worker_command.empty()) {
    node_manager_config.worker_commands.emplace(
        make_pair(Language::PYTHON, parse_worker_command(python_worker_command)));
  }
  if (!java_worker_command.empty()) {
    node_manager_config.worker_commands.emplace(
        make_pair(Language::JAVA, parse_worker_command(java_worker_command)));
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

  // Configuration for the object manager.
  ray::ObjectManagerConfig object_manager_config;
  object_manager_config.object_manager_port = object_manager_port;
  object_manager_config.store_socket_name = store_socket_name;
  object_manager_config.pull_timeout_ms =
      RayConfig::instance().object_manager_pull_timeout_ms();
  object_manager_config.push_timeout_ms =
      RayConfig::instance().object_manager_push_timeout_ms();

  int num_cpus = static_cast<int>(static_resource_conf["CPU"]);
  object_manager_config.max_sends = std::max(1, num_cpus / 4);
  object_manager_config.max_receives = std::max(1, num_cpus / 4);
  object_manager_config.object_chunk_size =
      RayConfig::instance().object_manager_default_chunk_size();

  RAY_LOG(DEBUG) << "Starting object manager with configuration: \n"
                 << "max_sends = " << object_manager_config.max_sends << "\n"
                 << "max_receives = " << object_manager_config.max_receives << "\n"
                 << "object_chunk_size = " << object_manager_config.object_chunk_size;

  // Initialize the node manager.
  boost::asio::io_service main_service;

  //  initialize mock gcs & object directory
  auto gcs_client = std::make_shared<ray::gcs::AsyncGcsClient>(redis_address, redis_port,
                                                               redis_password);
  RAY_LOG(DEBUG) << "Initializing GCS client "
                 << gcs_client->client_table().GetLocalClientId();

  std::unique_ptr<ray::raylet::Raylet> server(new ray::raylet::Raylet(
      main_service, raylet_socket_name, node_ip_address, redis_address, redis_port,
      redis_password, node_manager_config, object_manager_config, gcs_client));

  // Destroy the Raylet on a SIGTERM. The pointer to main_service is
  // guaranteed to be valid since this function will run the event loop
  // instead of returning immediately.
  // We should stop the service and remove the local socket file.
  auto handler = [&main_service, &raylet_socket_name, &server, &gcs_client](
      const boost::system::error_code &error, int signal_number) {
    auto shutdown_callback = [&server, &main_service]() {
      server.reset();
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
