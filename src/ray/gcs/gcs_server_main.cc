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

#include <cstdlib>
#include <iostream>
#include <limits>
#include <string>
#include <vector>

#include "gflags/gflags.h"
#include "ray/common/metrics.h"
#include "ray/common/ray_config.h"
#include "ray/gcs/gcs_server.h"
#include "ray/gcs/metrics.h"
#include "ray/gcs/store_client/redis_store_client.h"
#include "ray/observability/metrics.h"
#include "ray/stats/stats.h"
#include "ray/util/event.h"
#include "ray/util/raii.h"
#include "ray/util/stream_redirection.h"
#include "ray/util/stream_redirection_options.h"
#include "src/ray/protobuf/gcs_service.pb.h"

DEFINE_string(redis_address, "", "The ip address of redis.");
DEFINE_bool(redis_enable_ssl, false, "Use tls/ssl in redis connection.");
DEFINE_int32(redis_port, -1, "The port of redis.");
DEFINE_string(log_dir, "", "The path of the dir where log files are created.");
DEFINE_string(stdout_filepath, "", "The filepath to dump gcs server stdout.");
DEFINE_string(stderr_filepath, "", "The filepath to dump gcs server stderr.");
DEFINE_int32(gcs_server_port, 0, "The port of gcs server.");
DEFINE_int32(metrics_agent_port, -1, "The port of metrics agent.");
DEFINE_string(config_list, "", "The config list of gcs.");
DEFINE_string(redis_username, "", "The username of Redis.");
DEFINE_string(redis_password, "", "The password of Redis.");
DEFINE_bool(retry_redis, false, "Whether to retry to connect to Redis.");
DEFINE_string(node_ip_address, "", "The IP address of the node.");
DEFINE_string(session_name, "", "session_name: The current Ray session name.");
DEFINE_string(ray_commit, "", "The commit hash of Ray.");

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
  // By default, GCS server flushes all logging and stdout to a single file called
  // `gcs_server.out`, without log rotations. To keep backward compatibility at best
  // effort, we use the same filename as output, and disable log rotation by default.
  InitShutdownRAII ray_log_shutdown_raii(ray::RayLog::StartRayLog,
                                         ray::RayLog::ShutDownRayLog,
                                         argv[0],
                                         ray::RayLogLevel::INFO,
                                         /*log_filepath=*/"",
                                         /*err_log_filepath=*/"",
                                         /*log_rotation_max_size=*/0,
                                         /*log_rotation_file_num=*/1);
  ray::RayLog::InstallFailureSignalHandler(argv[0]);
  ray::RayLog::InstallTerminateHandler();

  RAY_LOG(INFO)
          .WithField("ray_version", kRayVersion)
          .WithField("ray_commit", FLAGS_ray_commit)
      << "Ray cluster metadata";

  const std::string redis_address = FLAGS_redis_address;
  const int redis_port = static_cast<int>(FLAGS_redis_port);
  const std::string log_dir = FLAGS_log_dir;
  const int gcs_server_port = static_cast<int>(FLAGS_gcs_server_port);
  const int metrics_agent_port = static_cast<int>(FLAGS_metrics_agent_port);
  std::string config_list;
  RAY_CHECK(absl::Base64Unescape(FLAGS_config_list, &config_list))
      << "config_list is not a valid base64-encoded string.";
  const std::string redis_password = FLAGS_redis_password;
  const std::string redis_username = FLAGS_redis_username;
  const bool retry_redis = FLAGS_retry_redis;
  const std::string node_ip_address = FLAGS_node_ip_address;
  const std::string session_name = FLAGS_session_name;
  gflags::ShutDownCommandLineFlags();

  RayConfig::instance().initialize(config_list);
  ray::asio::testing::Init();
  ray::rpc::testing::Init();

  // IO Service for main loop.
  SetThreadName("gcs_server");
  instrumented_io_context main_service(
      /*enable_metrics=*/RayConfig::instance().emit_main_service_metrics(),
      /*running_on_single_thread=*/true,
      "gcs_server_main_io_context");
  // Ensure that the IO service keeps running. Without this, the main_service will exit
  // as soon as there is no more work to be processed.
  boost::asio::executor_work_guard<boost::asio::io_context::executor_type> work(
      main_service.get_executor());

  ray::stats::enable_grpc_metrics_collection_if_needed("gcs");

  const ray::stats::TagsType global_tags = {{ray::stats::ComponentKey, "gcs_server"},
                                            {ray::stats::WorkerIdKey, ""},
                                            {ray::stats::VersionKey, kRayVersion},
                                            {ray::stats::NodeAddressKey, node_ip_address},
                                            {ray::stats::SessionNameKey, session_name}};
  ray::stats::Init(global_tags, metrics_agent_port, ray::WorkerID::Nil());

  // Initialize event framework.
  if (RayConfig::instance().event_log_reporter_enabled() && !log_dir.empty()) {
    // This GCS server process emits GCS standard events, and
    // Node, Actor, and Driver Job export events
    // so the various source types are passed to RayEventInit. The type of an
    // event is determined by the schema of its event data.
    const std::vector<ray::SourceTypeVariant> source_types = {
        ray::rpc::Event_SourceType::Event_SourceType_GCS,
        ray::rpc::ExportEvent_SourceType::ExportEvent_SourceType_EXPORT_NODE,
        ray::rpc::ExportEvent_SourceType_EXPORT_ACTOR,
        ray::rpc::ExportEvent_SourceType::ExportEvent_SourceType_EXPORT_DRIVER_JOB};
    ray::RayEventInit(source_types,
                      absl::flat_hash_map<std::string, std::string>(),
                      log_dir,
                      RayConfig::instance().event_level(),
                      RayConfig::instance().emit_event_to_log_file());
  }

  ray::gcs::GcsServerConfig gcs_server_config;
  gcs_server_config.grpc_server_name = "GcsServer";
  gcs_server_config.grpc_server_port = gcs_server_port;
  gcs_server_config.grpc_server_thread_num =
      RayConfig::instance().gcs_server_rpc_server_thread_num();
  gcs_server_config.metrics_agent_port = metrics_agent_port;
  gcs_server_config.redis_address = redis_address;
  gcs_server_config.redis_port = redis_port;
  gcs_server_config.enable_redis_ssl = FLAGS_redis_enable_ssl;
  gcs_server_config.redis_password = redis_password;
  gcs_server_config.redis_username = redis_username;
  gcs_server_config.retry_redis = retry_redis;
  gcs_server_config.node_ip_address = node_ip_address;
  gcs_server_config.metrics_agent_port = metrics_agent_port;
  gcs_server_config.log_dir = log_dir;
  gcs_server_config.raylet_config_list = config_list;
  gcs_server_config.session_name = session_name;

  // Create individual metrics
  auto actor_by_state_gauge = ray::GetActorByStateGaugeMetric();
  auto gcs_actor_by_state_gauge = ray::gcs::GetGcsActorByStateGaugeMetric();
  auto running_job_gauge = ray::gcs::GetRunningJobGaugeMetric();
  auto finished_job_counter = ray::gcs::GetFinishedJobCounterMetric();
  auto job_duration_in_seconds_gauge = ray::gcs::GetJobDurationInSecondsGaugeMetric();
  auto placement_group_gauge = ray::gcs::GetPlacementGroupGaugeMetric();
  auto placement_group_creation_latency_in_ms_histogram =
      ray::gcs::GetPlacementGroupCreationLatencyInMsHistogramMetric();
  auto placement_group_scheduling_latency_in_ms_histogram =
      ray::gcs::GetPlacementGroupSchedulingLatencyInMsHistogramMetric();
  auto placement_group_count_gauge = ray::gcs::GetPlacementGroupCountGaugeMetric();
  auto task_events_reported_gauge =
      ray::gcs::GetTaskManagerTaskEventsReportedGaugeMetric();
  auto task_events_dropped_gauge = ray::gcs::GetTaskManagerTaskEventsDroppedGaugeMetric();
  auto task_events_stored_gauge = ray::gcs::GetTaskManagerTaskEventsStoredGaugeMetric();
  auto event_recorder_dropped_events_counter =
      ray::GetRayEventRecorderDroppedEventsCounterMetric();
  auto storage_operation_latency_in_ms_histogram =
      ray::gcs::GetGcsStorageOperationLatencyInMsHistogramMetric();
  auto storage_operation_count_counter =
      ray::gcs::GetGcsStorageOperationCountCounterMetric();
  auto scheduler_placement_time_s_histogram =
      ray::GetSchedulerPlacementTimeSHistogramMetric();

  // Create the metrics struct
  ray::gcs::GcsServerMetrics gcs_server_metrics{
      /*actor_by_state_gauge=*/actor_by_state_gauge,
      /*gcs_actor_by_state_gauge=*/gcs_actor_by_state_gauge,
      /*running_job_gauge=*/running_job_gauge,
      /*finished_job_counter=*/finished_job_counter,
      /*job_duration_in_seconds_gauge=*/job_duration_in_seconds_gauge,
      /*placement_group_gauge=*/placement_group_gauge,
      /*placement_group_creation_latency_in_ms_histogram=*/
      placement_group_creation_latency_in_ms_histogram,
      /*placement_group_scheduling_latency_in_ms_histogram=*/
      placement_group_scheduling_latency_in_ms_histogram,
      /*placement_group_count_gauge=*/placement_group_count_gauge,
      /*task_events_reported_gauge=*/task_events_reported_gauge,
      /*task_events_dropped_gauge=*/task_events_dropped_gauge,
      /*task_events_stored_gauge=*/task_events_stored_gauge,
      /*event_recorder_dropped_events_counter=*/event_recorder_dropped_events_counter,
      /*storage_operation_latency_in_ms_histogram=*/
      storage_operation_latency_in_ms_histogram,
      /*storage_operation_count_counter=*/storage_operation_count_counter,
      scheduler_placement_time_s_histogram,
  };

  ray::gcs::GcsServer gcs_server(gcs_server_config, gcs_server_metrics, main_service);

  // Destroy the GCS server on a SIGTERM. The pointer to main_service is
  // guaranteed to be valid since this function will run the event loop
  // instead of returning immediately.
  auto handler = [&main_service, &gcs_server](const boost::system::error_code &error,
                                              int signal_number) {
    RAY_LOG(INFO) << "GCS server received SIGTERM, shutting down...";
    main_service.stop();
    ray::rpc::DrainServerCallExecutor();
    gcs_server.Stop();
    ray::stats::Shutdown();
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
