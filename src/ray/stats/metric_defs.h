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

#include "ray/stats/metric.h"

namespace ray {

namespace stats {

/// The definitions of metrics that you can use everywhere.
///
/// There are 4 types of metric. The values of the metrics are of type double.
///   Histogram: Histogram distribution of metric points.
///   Gauge: Keeps the last recorded value, drops everything before.
///   Count: The count of the number of metric points.
///   Sum: A sum up of the metric points.
///
/// You can follow these examples to define your metrics.

/// NOTE: When adding a new metric, add the metric name to the _METRICS list in
/// python/ray/tests/test_metrics_agent.py to ensure that its existence is tested.

/// Convention
/// Following Prometheus convention
/// https://prometheus.io/docs/concepts/data_model/#metric-names-and-labels
/// Units: ray_[component]_[metrics_name]_[units] (e.g.,
/// ray_pull_manager_spill_throughput_mb) Gauge: ray_[component]_[metrics_name]s (e.g.,
/// ray_pull_manager_requests) Cumulative Gauge / Count:
/// ray_[component]_[metrics_name]_total (e.g., ray_pull_manager_total)
///

}  // namespace stats

}  // namespace ray
