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

#pragma once

#include <exception>
#include <string>
#include <unordered_map>
#include <vector>

#include "absl/synchronization/mutex.h"
#include "grpcpp/opencensus.h"
#include "opencensus/stats/internal/delta_producer.h"
#include "opencensus/stats/stats.h"
#include "opencensus/tags/tag_key.h"
#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/asio/io_service_pool.h"
#include "ray/common/id.h"
#include "ray/common/ray_config.h"
#include "ray/observability/open_telemetry_metric_recorder.h"
#include "ray/stats/metric.h"
#include "ray/stats/metric_exporter.h"
#include "ray/util/logging.h"
#include "ray/util/network_util.h"

namespace ray {

namespace stats {

#include <boost/asio.hpp>

using OpenTelemetryMetricRecorder = ray::observability::OpenTelemetryMetricRecorder;

// TODO(sang) Put all states and logic into a singleton class Stats.
static std::shared_ptr<IOServicePool> metrics_io_service_pool;
static absl::Mutex stats_mutex;

// Returns true if OpenCensus should be enabled.
static inline bool should_enable_open_census() {
  return !RayConfig::instance().enable_open_telemetry() ||
         !RayConfig::instance().enable_grpc_metrics_collection_for().empty();
}

/// Initialize stats for a process.
/// NOTE:
/// - stats::Init should be called only once per PROCESS. Redundant calls will be just
/// ignored.
/// - If you want to reinitialize, you should call stats::Shutdown().
/// - It is thread-safe.
/// We recommend you to use this only once inside a main script and add Shutdown() method
/// to any signal handler.
/// \param global_tags[in] Tags that will be appended to all metrics in this process.
/// \param metrics_agent_port[in] The port to export metrics at each node.
/// \param worker_id[in] The worker ID of the current component.
static inline void Init(
    const TagsType &global_tags,
    const int metrics_agent_port,
    const WorkerID &worker_id,
    int64_t metrics_report_batch_size = RayConfig::instance().metrics_report_batch_size(),
    int64_t max_grpc_payload_size = RayConfig::instance().agent_max_grpc_message_size()) {
  absl::MutexLock lock(&stats_mutex);
  if (StatsConfig::instance().IsInitialized()) {
    return;
  }

  RAY_CHECK(metrics_io_service_pool == nullptr);
  bool disable_stats = !RayConfig::instance().enable_metrics_collection();
  StatsConfig::instance().SetIsDisableStats(disable_stats);
  if (disable_stats) {
    RAY_LOG(INFO) << "Disabled stats.";
    return;
  }
  RAY_LOG(DEBUG) << "Initialized stats";

  // Set interval.
  StatsConfig::instance().SetReportInterval(absl::Milliseconds(std::max(
      RayConfig::instance().metrics_report_interval_ms(), static_cast<uint64_t>(1000))));
  StatsConfig::instance().SetHarvestInterval(
      absl::Milliseconds(std::max(RayConfig::instance().metrics_report_interval_ms() / 2,
                                  static_cast<uint64_t>(500))));
  // Register the metric recorder.
  if (should_enable_open_census()) {
    metrics_io_service_pool = std::make_shared<IOServicePool>(1);
    metrics_io_service_pool->Run();
    instrumented_io_context *metrics_io_service = metrics_io_service_pool->Get();
    RAY_CHECK(metrics_io_service != nullptr);
    opencensus::stats::StatsExporter::SetInterval(
        StatsConfig::instance().GetReportInterval());
    opencensus::stats::DeltaProducer::Get()->SetHarvestInterval(
        StatsConfig::instance().GetHarvestInterval());
    OpenCensusProtoExporter::Register(metrics_agent_port,
                                      (*metrics_io_service),
                                      worker_id,
                                      metrics_report_batch_size,
                                      max_grpc_payload_size);
  }

  StatsConfig::instance().SetGlobalTags(global_tags);
  for (auto &f : StatsConfig::instance().PopInitializers()) {
    f();
  }
  StatsConfig::instance().SetIsInitialized(true);
}

static inline void InitOpenTelemetryExporter(const int metrics_agent_port,
                                             const Status &metrics_agent_server_status) {
  if (!RayConfig::instance().enable_open_telemetry()) {
    return;
  }
  if (!metrics_agent_server_status.ok()) {
    RAY_LOG(ERROR) << "Failed to initialize OpenTelemetry exporter. Data will not be "
                      "exported to the "
                   << "metrics agent. Server status: " << metrics_agent_server_status;
    return;
  }
  OpenTelemetryMetricRecorder::GetInstance().RegisterGrpcExporter(
      /*endpoint=*/std::string("127.0.0.1:") + std::to_string(metrics_agent_port),
      /*interval=*/
      std::chrono::milliseconds(
          absl::ToInt64Milliseconds(StatsConfig::instance().GetReportInterval())),
      /*timeout=, set the timeout to be half of the interval to avoid potential request
         queueing.*/
      std::chrono::milliseconds(
          absl::ToInt64Milliseconds(0.5 * StatsConfig::instance().GetReportInterval())));
}

/// Shutdown the initialized stats library.
/// This cleans up various threads and metadata for stats library.
static inline void Shutdown() {
  absl::MutexLock lock(&stats_mutex);
  if (!StatsConfig::instance().IsInitialized()) {
    // Return if stats had never been initialized.
    return;
  }
  if (RayConfig::instance().enable_open_telemetry()) {
    OpenTelemetryMetricRecorder::GetInstance().Shutdown();
  }
  if (should_enable_open_census()) {
    metrics_io_service_pool->Stop();
    opencensus::stats::DeltaProducer::Get()->Shutdown();
    opencensus::stats::StatsExporter::Shutdown();
    metrics_io_service_pool = nullptr;
  }
  StatsConfig::instance().SetIsInitialized(false);
  RAY_LOG(INFO) << "Stats module has shutdown.";
}

// Enables grpc metrics collection if `my_component` is in
// `RAY_enable_grpc_metrics_collection_for`.
// Must be called early, before any grpc call, or the program crashes.
static inline void enable_grpc_metrics_collection_if_needed(
    std::string_view my_component) {
  if (!RayConfig::instance().enable_grpc_metrics_collection_for().empty()) {
    std::vector<std::string> results;
    boost::split(results,
                 RayConfig::instance().enable_grpc_metrics_collection_for(),
                 boost::is_any_of(","));
    if (std::find(results.begin(), results.end(), my_component) != results.end()) {
      grpc::RegisterOpenCensusPlugin();
      grpc::RegisterOpenCensusViewsForExport();
    }
  }
}

}  // namespace stats

}  // namespace ray
