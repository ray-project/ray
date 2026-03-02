// Copyright 2026 The Ray Authors.
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

#include "ray/stats/percentile_metric.h"

#include <cmath>

namespace ray {
namespace stats {

void PercentileMetric::Record(double value) {
  absl::MutexLock lock(&mutex_);
  tracker_.Record(value);
}

void PercentileMetric::Record(double value, stats::TagsType /*tags*/) {
  RAY_CHECK(false) << "PercentileMetric does not support per-record tags. Use separate "
                      "PercentileMetric instances per workload type instead.";
}

void PercentileMetric::Record(
    double value, std::vector<std::pair<std::string_view, std::string>> /*tags*/) {
  RAY_CHECK(false) << "PercentileMetric does not support per-record tags. Use separate "
                      "PercentileMetric instances per workload type instead.";
}

void PercentileMetric::Flush() {
  // Atomically swap out the histogram (under lock).
  std::unique_ptr<BucketHistogram> old_histogram;
  {
    absl::MutexLock lock(&mutex_);
    old_histogram = tracker_.GetAndFlushHistogram();
  }

  if (old_histogram->GetCount() == 0) {
    // No samples this window: reset gauges to 0 so stale values from a prior
    // window are not mistaken for recent observations.
    p50_gauge_.Record(0);
    p95_gauge_.Record(0);
    p99_gauge_.Record(0);
    max_gauge_.Record(0);
    mean_gauge_.Record(0);
    return;
  }

  double p50 = old_histogram->GetPercentile(0.50);
  double p95 = old_histogram->GetPercentile(0.95);
  double p99 = old_histogram->GetPercentile(0.99);
  double max_val = old_histogram->GetMax();
  double mean = old_histogram->GetMean();

  if (!std::isnan(p50)) {
    p50_gauge_.Record(p50);
  }
  if (!std::isnan(p95)) {
    p95_gauge_.Record(p95);
  }
  if (!std::isnan(p99)) {
    p99_gauge_.Record(p99);
  }
  if (!std::isnan(max_val)) {
    max_gauge_.Record(max_val);
  }
  if (!std::isnan(mean)) {
    mean_gauge_.Record(mean);
  }
}

void PercentileMetric::Clear() {
  absl::MutexLock lock(&mutex_);
  tracker_.Clear();
}

}  // namespace stats
}  // namespace ray
