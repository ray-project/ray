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

#include "opencensus/exporters/stats/prometheus/prometheus_exporter.h"
#include "opencensus/exporters/stats/stdout/stdout_exporter.h"
#include "opencensus/stats/internal/delta_producer.h"
#include "opencensus/stats/stats.h"
#include "opencensus/tags/tag_key.h"
#include "ray/common/ray_config.h"
#include "ray/stats/metric.h"
#include "ray/stats/metric_exporter.h"
#include "ray/stats/metric_exporter_client.h"
#include "ray/util/logging.h"

namespace ray {

namespace stats {

#include <boost/asio.hpp>

/// Include metric_defs.h to define measure items.
#include "ray/stats/metric_defs.h"

/// Initialize stats.
static inline void Init(
    const TagsType &global_tags, const int metrics_agent_port,
    boost::asio::io_service &io_service,
    std::shared_ptr<MetricExporterClient> exporter_to_use = nullptr,
    int64_t metrics_report_batch_size = RayConfig::instance().metrics_report_batch_size(),
    bool disable_stats = !RayConfig::instance().enable_metrics_collection()) {
  StatsConfig::instance().SetIsDisableStats(disable_stats);
  if (disable_stats) {
    RAY_LOG(INFO) << "Disabled stats.";
    return;
  }

  // Force to have a singleton exporter.
  static std::shared_ptr<MetricExporterClient> exporter;
  // Default exporter is metrics agent exporter.
  if (exporter_to_use == nullptr) {
    std::shared_ptr<MetricExporterClient> stdout_exporter(new StdoutExporterClient());
    exporter.reset(new MetricsAgentExporter(stdout_exporter, metrics_agent_port,
                                            io_service, "127.0.0.1"));
  } else {
    exporter = exporter_to_use;
  }

  // TODO(sang): Currently, we don't do any cleanup. This can lead us to lose last 10
  // seconds data before we exit the main script.
  MetricExporter::Register(exporter, metrics_report_batch_size);
  opencensus::stats::StatsExporter::SetInterval(
      StatsConfig::instance().GetReportInterval());
  opencensus::stats::DeltaProducer::Get()->SetHarvestInterval(
      StatsConfig::instance().GetHarvestInterval());
  StatsConfig::instance().SetGlobalTags(global_tags);
}

}  // namespace stats

}  // namespace ray
