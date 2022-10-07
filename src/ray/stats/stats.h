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

#include "absl/synchronization/mutex.h"
#include "opencensus/stats/internal/delta_producer.h"
#include "opencensus/stats/stats.h"
#include "opencensus/tags/tag_key.h"
#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/asio/io_service_pool.h"
#include "ray/common/id.h"
#include "ray/common/ray_config.h"
#include "ray/stats/metric.h"
#include "ray/stats/metric_exporter.h"
#include "ray/stats/metric_exporter_client.h"
#include "ray/util/logging.h"

namespace ray {

namespace stats {

#include <boost/asio.hpp>

// TODO(sang) Put all states and logic into a singleton class Stats.
static std::shared_ptr<IOServicePool> metrics_io_service_pool;
static std::shared_ptr<MetricExporterClient> exporter;
static absl::Mutex stats_mutex;

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
/// \param exporter_to_use[in] The exporter client you will use for this process' metrics.
static inline void Init(const TagsType &global_tags,
                        const int metrics_agent_port,
                        const WorkerID &worker_id,
                        std::shared_ptr<MetricExporterClient> exporter_to_use = nullptr,
                        int64_t metrics_report_batch_size =
                            RayConfig::instance().metrics_report_batch_size()) {
  absl::MutexLock lock(&stats_mutex);
  if (StatsConfig::instance().IsInitialized()) {
    RAY_CHECK(metrics_io_service_pool != nullptr);
    RAY_CHECK(exporter != nullptr);
    return;
  }

  RAY_CHECK(metrics_io_service_pool == nullptr);
  RAY_CHECK(exporter == nullptr);
  bool disable_stats = !RayConfig::instance().enable_metrics_collection();
  StatsConfig::instance().SetIsDisableStats(disable_stats);
  if (disable_stats) {
    RAY_LOG(INFO) << "Disabled stats.";
    return;
  }
  RAY_LOG(DEBUG) << "Initialized stats";

  metrics_io_service_pool = std::make_shared<IOServicePool>(1);
  metrics_io_service_pool->Run();
  instrumented_io_context *metrics_io_service = metrics_io_service_pool->Get();
  RAY_CHECK(metrics_io_service != nullptr);

  // Default exporter is a metrics agent exporter.
  if (exporter_to_use == nullptr) {
    std::shared_ptr<MetricExporterClient> stdout_exporter(new StdoutExporterClient());
    exporter.reset(new MetricsAgentExporter(stdout_exporter));
  } else {
    exporter = exporter_to_use;
  }

  // Set interval.
  StatsConfig::instance().SetReportInterval(absl::Milliseconds(std::max(
      RayConfig::instance().metrics_report_interval_ms(), static_cast<uint64_t>(1000))));
  StatsConfig::instance().SetHarvestInterval(
      absl::Milliseconds(std::max(RayConfig::instance().metrics_report_interval_ms() / 2,
                                  static_cast<uint64_t>(500))));

  MetricPointExporter::Register(exporter, metrics_report_batch_size);
  OpenCensusProtoExporter::Register(
      metrics_agent_port, (*metrics_io_service), "127.0.0.1", worker_id);
  opencensus::stats::StatsExporter::SetInterval(
      StatsConfig::instance().GetReportInterval());
  opencensus::stats::DeltaProducer::Get()->SetHarvestInterval(
      StatsConfig::instance().GetHarvestInterval());
  StatsConfig::instance().SetGlobalTags(global_tags);
  for (auto &f : StatsConfig::instance().PopInitializers()) {
    f();
  }
  StatsConfig::instance().SetIsInitialized(true);
}

/// Shutdown the initialized stats library.
/// This cleans up various threads and metadata for stats library.
static inline void Shutdown() {
  absl::MutexLock lock(&stats_mutex);
  if (!StatsConfig::instance().IsInitialized()) {
    // Return if stats had never been initialized.
    return;
  }
  metrics_io_service_pool->Stop();
  opencensus::stats::DeltaProducer::Get()->Shutdown();
  opencensus::stats::StatsExporter::Shutdown();
  metrics_io_service_pool = nullptr;
  exporter = nullptr;
  StatsConfig::instance().SetIsInitialized(false);
}

}  // namespace stats

}  // namespace ray
