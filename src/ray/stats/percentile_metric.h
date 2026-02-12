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

  - Node A: 1000 requests, ranging 1-10ms → P95 = 10ms
  - Node B: 10 requests, ranging 1000-2000ms → P95 = 1900ms
  - Average of P95s: (10 + 1900) / 2 = 955ms

  But the true P95 of all 1010 requests combined is ~10ms (because 950 requests are still
  under 10ms).
 */
class PercentileMetric {
 public:
  /**
    Create a percentile metric.

    Uses linear binning optimized for long-tail distributions (typical for latencies).

    @param name Base metric name (e.g., "task_prepare_time").
    @param description Metric description.
    @param unit Unit of measurement (e.g., "ms").
    @param max_expected_value Maximum expected value for histogram binning (e.g., 10000.0
    for 10 seconds).
    @param memory_bytes Approximate memory usage (default: 1KB = 256 bins).
    @return Shared pointer to the created PercentileMetric.
   */
  static std::shared_ptr<PercentileMetric> Create(const std::string &name,
                                                  const std::string &description,
                                                  const std::string &unit,
                                                  double max_expected_value,
                                                  int memory_bytes = 1024) {
    auto tracker = PercentileTracker::Create(memory_bytes, max_expected_value);
    auto metric = std::shared_ptr<PercentileMetric>(
        new PercentileMetric(name, description, unit, std::move(tracker)));

    // Register this metric for periodic flushing
    RegisterMetricForFlushing(metric);

    return metric;
  }

  /**
    Flush all registered PercentileMetric instances.

    This should be called periodically (e.g., every few seconds) to update all percentile
    gauge metrics. This is typically called by the metrics collection/export system.
   */
  static void FlushAll() {
    absl::MutexLock lock(&registry_mutex_);
    for (auto it = registered_metrics_.begin(); it != registered_metrics_.end();) {
      if (auto metric = it->lock()) {
        metric->Flush();
        ++it;
      } else {
        // Metric has been destroyed, remove from registry
        it = registered_metrics_.erase(it);
      }
    }
  }

  /**
    Record a latency value.

    This method is thread-safe. Percentile gauges are NOT updated immediately;
    they are updated only when Flush() is called or during periodic metric collection.

    @param value The latency value to record.
   */
  void Record(double value) {
    absl::MutexLock lock(&mutex_);
    tracker_.Record(value);
  }

  /**
    Calculate current percentiles and update gauge metrics.

    This should be called periodically (e.g., every few seconds) rather than on every
    Record() call to avoid performance overhead. Typically called by the metrics
    collection/export system.

    This method uses a lock-free double-buffering approach:
    1. Quickly swap out the current histogram for a new empty one (under lock, ~50ns)
    2. Calculate percentiles from the old histogram (no lock, ~5-10µs)
    3. Update gauges and discard the old histogram

    This ensures Record() calls are only blocked for the pointer swap, not the expensive
    percentile calculations.

    This method is thread-safe.
   */
  void Flush() {
    // Step 1: Atomically swap out the histogram (fast, under lock)
    std::unique_ptr<PercentileHistogram> old_histogram;
    {
      absl::MutexLock lock(&mutex_);
      old_histogram = tracker_.SwapHistogram();
    }
    // Lock released here - new Record() calls go to the new histogram

    // Step 2: Calculate percentiles from old histogram (no lock needed)
    if (old_histogram->GetCount() == 0) {
      // No data to flush
      return;
    }

    double p50 = old_histogram->GetPercentile(0.50);
    double p95 = old_histogram->GetPercentile(0.95);
    double p99 = old_histogram->GetPercentile(0.99);
    double max_val = old_histogram->GetMax();
    double mean = old_histogram->GetMean();

    // Step 3: Update gauges (gauges have their own thread-safety)
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

    // old_histogram is automatically destroyed here
  }

  /**
    Clear all recorded values and reset gauges.

    This method is thread-safe.
   */
  void Clear() {
    absl::MutexLock lock(&mutex_);
    tracker_.Clear();
    // Note: We don't update gauges here - they'll be NaN until new data arrives and
    // Flush() is called
  }

  /**
    Get the base metric name.

    @return The base metric name (without _p50, _p95, etc. suffixes).
   */
  const std::string &GetName() const { return name_; }

 private:
  /**
    Construct a PercentileMetric.

    Use CreateForLatency() to create instances.

    @param name Base metric name.
    @param description Metric description.
    @param unit Unit of measurement.
    @param tracker The percentile tracker to use.
   */
  PercentileMetric(const std::string &name,
                   const std::string &description,
                   const std::string &unit,
                   PercentileTracker tracker)
      : name_(name),
        description_(description),
        unit_(unit),
        tracker_(std::move(tracker)),
        p50_gauge_(name + "_p50", description + " (P50)", unit),
        p95_gauge_(name + "_p95", description + " (P95)", unit),
        p99_gauge_(name + "_p99", description + " (P99)", unit),
        max_gauge_(name + "_max", description + " (Max)", unit),
        mean_gauge_(name + "_mean", description + " (Mean)", unit) {}

  std::string name_;
  std::string description_;
  std::string unit_;

  absl::Mutex mutex_;
  PercentileTracker tracker_ ABSL_GUARDED_BY(mutex_);

  // Gauge metrics for each percentile
  Gauge p50_gauge_;
  Gauge p95_gauge_;
  Gauge p99_gauge_;
  Gauge max_gauge_;
  Gauge mean_gauge_;

  // Static registry for periodic flushing
  static void RegisterMetricForFlushing(std::shared_ptr<PercentileMetric> metric) {
    absl::MutexLock lock(&registry_mutex_);
    registered_metrics_.push_back(std::weak_ptr<PercentileMetric>(metric));
  }

  inline static absl::Mutex registry_mutex_;
  inline static std::vector<std::weak_ptr<PercentileMetric>> registered_metrics_
      ABSL_GUARDED_BY(registry_mutex_);
};

}  // namespace stats
}  // namespace ray
