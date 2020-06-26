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
#include "opencensus/stats/stats.h"
#include "opencensus/tags/tag_key.h"
#include "prometheus/exposer.h"

#include "ray/stats/metric.h"
#include "ray/util/logging.h"

namespace ray {

namespace stats {

/// Include metric_defs.h to define measure items.
#include "metric_defs.h"

/// Initialize stats.
static void Init(const std::string &address, const TagsType &global_tags,
                 bool disable_stats = false, bool enable_stdout_exporter = false) {
  StatsConfig::instance().SetIsDisableStats(disable_stats);
  if (disable_stats) {
    RAY_LOG(INFO) << "Disabled stats.";
    return;
  }

  // Enable the Prometheus exporter.
  // Note that the reason for we using local static variables
  // here is to make sure they are single-instances.
  static auto exporter =
      std::make_shared<opencensus::exporters::stats::PrometheusExporter>();

  if (enable_stdout_exporter) {
    // Enable stdout exporter by default.
    opencensus::exporters::stats::StdoutExporter::Register();
  }

  // Enable prometheus exporter.
  try {
    static prometheus::Exposer exposer(address);
    exposer.RegisterCollectable(exporter);
    RAY_LOG(INFO) << "Succeeded to initialize stats: exporter address is " << address;
  } catch (std::exception &e) {
    RAY_LOG(WARNING) << "Failed to create the Prometheus exporter. This doesn't "
                     << "affect anything except stats. Caused by: " << e.what();
    return;
  }

  StatsConfig::instance().SetGlobalTags(global_tags);
}

}  // namespace stats

}  // namespace ray
