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
//
// This implementation is based on tehuti's Percentiles class:
// https://github.com/tehuti-io/tehuti
// Original implementation: Copyright LinkedIn Corp. under Apache License 2.0

#pragma once

#include <memory>
#include <string>
#include <vector>

#include "absl/synchronization/mutex.h"
#include "ray/observability/metric_interface.h"
#include "ray/stats/metric.h"
#include "ray/stats/percentile_tracker.h"

namespace ray {
namespace stats {

/**
  @class PercentileMetric

  Percentile-based metric that tracks and emits p50, p95, p99, max, and mean as separate
  gauge metrics for input samples.

  How to use these for diagnosis:

  mean and p50 tell you roughly what most samples are being measured at.  However, if
  mean >> p50, this tells you that there are outliers that are significantly impacting the
  measure.  At which point, you should examine P95/P99 tail measures.

  P95/P99 are the tail measures. More importantly, when the system starts to degrade,
  these are the measures which will trigger FIRST and more sensitively.  You should be
  using these for alerting (as they will trigger faster).

  Max is what you should use if you're convinced there's at least one sample out there
  that is causing an issue (think in terms of a single large request or some kind of
  system event).

  N.B. DO NOT AGGREGATE THESE METRICS:
  The gauge metrics emitted by this class are LOCAL to this process/node and should never
  be aggregated.  As such, do not average or sum them or calculate percentiles. Doing so
  is statistically invalid and will produce misleading results.

  Why is that?  Well, consider the below example:

  - Node A: 1000 requests, ranging 1-10ms -> P95 = 10ms
  - Node B: 10 requests, ranging 1000-2000ms -> P95 = 1900ms
  - Average of P95s: (10 + 1900) / 2 = 955ms

  But the true P95 of all 1010 requests combined is ~10ms (because 950 requests are still
  under 10ms).

  Flushing:
  The owner of this class is responsible for calling Flush() periodically (e.g., from
  RecordMetrics()) to push accumulated percentile data into the exported gauges.
 */
class PercentileMetric : public ray::observability::MetricInterface {
 public:
  /**
    Construct a percentile metric.

    Uses quadratic bucketing optimized for long-tail distributions (typical for
    latencies).

    @param name Base metric name (e.g., "task_prepare_time").
    @param description Metric description.
    @param unit Unit of measurement (e.g., "ms").
    @param max_expected_value Maximum expected value for histogram bucketing (e.g.,
    10000.0 for 10 seconds).
    @param num_buckets Number of buckets (default: 256). More buckets = finer precision.
   */
  PercentileMetric(const std::string &name,
                   const std::string &description,
                   const std::string &unit,
                   double max_expected_value,
                   int num_buckets = 256)
      : tracker_(PercentileTracker::Create(num_buckets, max_expected_value)),
        p50_gauge_(name + "_p50", description + " (P50)", unit),
        p95_gauge_(name + "_p95", description + " (P95)", unit),
        p99_gauge_(name + "_p99", description + " (P99)", unit),
        max_gauge_(name + "_max", description + " (Max)", unit),
        mean_gauge_(name + "_mean", description + " (Mean)", unit) {}

  /**
    Record a latency value.

    This method is thread-safe. Percentile gauges are NOT updated immediately;
    they are updated only when Flush() is called during periodic metric collection.

    @param value The latency value to record.
   */
  void Record(double value) override;

  void Record(double value, stats::TagsType /*tags*/) override;

  void Record(double value,
              std::vector<std::pair<std::string_view, std::string>> /*tags*/) override;

  /**
    Calculate current percentiles and update gauge metrics.

    This uses a double-buffering approach: quickly swap out the current histogram
    for a new empty one (under lock), then calculate percentiles from the old
    histogram outside the lock. This ensures Record() calls are only blocked for
    the pointer swap, not the percentile calculations.

    This method is thread-safe.
   */
  void Flush() override;

  /**
    Clear all recorded values.
   */
  void Clear();

 private:
  absl::Mutex mutex_;
  PercentileTracker tracker_ ABSL_GUARDED_BY(mutex_);

  // Gauge metrics for each percentile
  Gauge p50_gauge_;
  Gauge p95_gauge_;
  Gauge p99_gauge_;
  Gauge max_gauge_;
  Gauge mean_gauge_;
};

}  // namespace stats
}  // namespace ray
