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

#include <cstdint>
#include <memory>

#include "ray/stats/bucket_histogram.h"
#include "ray/stats/quadratic_bucket_scheme.h"
#include "ray/util/logging.h"

namespace ray {
namespace stats {

/**
  @class PercentileTracker

  Tracks latency percentiles using histogram-based approximation.

  IMPORTANT - AGGREGATION WARNING:
  Percentile values from this tracker CANNOT be meaningfully aggregated across nodes.
  DO NOT average, sum, or otherwise combine P50/P95/P99 values from multiple trackers.

  Why? Percentiles are order statistics based on rank position in the data distribution.
  Aggregating percentiles loses critical information about sample sizes and underlying
  distributions, leading to incorrect results.

  Example: If Node A has P95=10ms (1000 samples) and Node B has P95=1900ms (10 samples),
  averaging gives 955ms, but the true combined P95 is ~10ms.

  This tracker is designed for per-node observability, not cross-node aggregation.
 */
class PercentileTracker {
 public:
  /**
    Create a percentile tracker optimized for latency measurements.

    @param num_buckets Number of buckets. More buckets = finer precision, more memory
    (each bucket is sizeof(float) = 4 bytes). Must be >= 2.
    @param max_value Maximum expected value for bucket range. Must be positive.
    @return A PercentileTracker configured for latency tracking.
   */
  static PercentileTracker Create(int num_buckets, double max_value) {
    RAY_CHECK(num_buckets >= 2) << "num_buckets must be >= 2, got " << num_buckets;
    RAY_CHECK(max_value > 0.0) << "max_value must be positive, got " << max_value;
    return PercentileTracker(num_buckets, max_value);
  }

  // Non-copyable: each tracker owns its histogram state.
  PercentileTracker(const PercentileTracker &) = delete;
  PercentileTracker &operator=(const PercentileTracker &) = delete;

  PercentileTracker(PercentileTracker &&) = default;
  PercentileTracker &operator=(PercentileTracker &&) = default;

  /**
    Record a latency value.

    @param value The latency value to record (e.g., milliseconds).
   */
  void Record(double value) { histogram_->Record(value); }

  /**
    Clear all recorded values.
   */
  void Clear() { histogram_->Clear(); }

  double GetP50() const { return histogram_->GetPercentile(0.50); }
  double GetP95() const { return histogram_->GetPercentile(0.95); }
  double GetP99() const { return histogram_->GetPercentile(0.99); }
  double GetMax() const { return histogram_->GetMax(); }
  double GetMean() const { return histogram_->GetMean(); }

  /**
    Get the total number of values recorded.

    @return Count of recorded values.
   */
  int64_t GetCount() const { return histogram_->GetCount(); }

  std::unique_ptr<BucketHistogram> GetAndFlushHistogram();

 private:
  PercentileTracker(int num_buckets, double max_value)
      : bucket_scheme_(std::make_unique<QuadraticBucketScheme>(num_buckets, max_value)),
        histogram_(std::make_unique<BucketHistogram>(bucket_scheme_.get())) {}

  std::unique_ptr<QuadraticBucketScheme> bucket_scheme_;
  std::unique_ptr<BucketHistogram> histogram_;
};

}  // namespace stats
}  // namespace ray
