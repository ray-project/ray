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
//
// ─── Math reference ─────────────────────────────────────────────────────────
//
// BUCKET BOUNDARIES
//   boundary(b) = scale * b * (b+1) / 2        (b = 0, 1, 2, …, N-1)
//   scale = max_value / (N * (N-1) / 2)
//   Bucket b contains values in [boundary(b), boundary(b+1)).
//   Bucket widths grow linearly (b0 width = scale, b1 width = 2*scale, …),
//   giving finer resolution where most latencies live.
//
// TOBUCKET (inverse)
//   Given value v, find b such that b*(b+1)/2 ≤ v/scale < (b+1)*(b+2)/2.
//   Solving the quadratic b*(b+1)/2 = v/scale:
//     b = floor( -0.5 + sqrt(2*v/scale + 0.25) )
//   std::min clamps against floating-point rounding at bucket boundaries.
//
// GETPERCENTILE
//   Scan buckets left to right, accumulating counts.  Return FromBucket(i)
//   for the first i where cumulative_count / total_count > quantile.
//   Error bound: ≤ one bucket width at the returned percentile value.
//   With N buckets and max_value M, width at bucket i ≈ M*i / (N*(N-1)/2).
//
// GETMEAN
//   Weighted average of bucket representative values (lower boundary for all
//   but the last bucket, max_value for the overflow bucket).  Slightly
//   underestimates the true mean when values cluster near the top of a bucket.
//
// ─── Worked example ─────────────────────────────────────────────────────────
//
//   N=10 buckets, max_value=10s → scale = 10/(10*9/2) ≈ 0.222s
//   Boundaries: b0=0, b1=0.22, b2=0.67, b3=1.33, b4=2.22, b5=3.33, …
//
//   First sample: v=10s → ToBucket returns 9 (overflow bucket, clamped).
//   Next samples cluster at 1–2s → ToBucket returns 3 or 4.
//   After many such samples, P95 resolves into the 1–2s range even though
//   the very first spike landed in the overflow bucket.
//
// ────────────────────────────────────────────────────────────────────────────

#pragma once

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <memory>
#include <vector>

#include "ray/util/logging.h"

namespace ray {
namespace stats {

/**
  @class QuadraticBucketScheme

  Bucketing scheme where bucket boundaries grow quadratically, providing finer
  resolution for smaller values. This is useful for latency distributions
  where most values are small but occasional spikes can be large.

  Bucket boundaries follow the formula: boundary = scale * b * (b + 1) / 2
  Bucket widths grow linearly: 1x, 2x, 3x, 4x...

  This provides fine granularity where most data is (low latencies) and coarse
  granularity for rare outliers (high latencies).
 */
class QuadraticBucketScheme {
 public:
  /**
    Create a quadratic bucket scheme.

    @param num_buckets Number of buckets to create. Must be >= 2.
    @param max_value Maximum expected value for bucketing. Must be positive.
   */
  QuadraticBucketScheme(int num_buckets, double max_value)
      : num_buckets_(num_buckets),
        max_value_(max_value),
        scale_(max_value / (num_buckets * (num_buckets - 1) / 2.0)) {
    RAY_CHECK(num_buckets >= 2) << "num_buckets must be >= 2, got " << num_buckets;
    RAY_CHECK(max_value > 0.0) << "max_value must be positive, got " << max_value;
  }

  /**
    Get the total number of buckets.

    @return Number of buckets.
   */
  int NumBuckets() const { return num_buckets_; }

  /**
    Map a value to its corresponding bucket index.

    @param value The value to map to a bucket.
    @return The bucket index for this value, clamped to [0, num_buckets - 1].
   */
  int ToBucket(double value) const {
    if (value <= 0.0) {
      return 0;
    } else if (value >= max_value_) {
      return num_buckets_ - 1;
    } else {
      double scaled = value / scale_;
      // Solve quadratic: b * (b + 1) / 2 = scaled
      int bucket = static_cast<int>(-0.5 + std::sqrt(2.0 * scaled + 0.25));
      // Clamp to guard against returning a value greater than num_buckets_ - 1
      return std::min(bucket, num_buckets_ - 1);
    }
  }

  /**
    Get the representative value for a bucket index.

    For buckets 0 through num_buckets-2 this returns the lower boundary of the
    bucket. For the last bucket (overflow bucket) this returns max_value.

    @param bucket The bucket index.
    @return The representative value for this bucket.
   */
  double FromBucket(int bucket) const;

 private:
  int num_buckets_;
  double max_value_;
  double scale_;
};

/**
  @class BucketHistogram

  Lightweight histogram for percentile tracking using bucket-based approximation.
 */
class BucketHistogram {
 public:
  /**
    Create a histogram with the specified bucketing scheme.

    @param bucket_scheme The bucketing scheme to use. Must outlive this histogram.
   */
  explicit BucketHistogram(const QuadraticBucketScheme *bucket_scheme)
      : bucket_scheme_(bucket_scheme),
        counts_(bucket_scheme->NumBuckets(), 0.0f),
        count_(0) {}

  /**
    Record a value in the histogram.

    @param value The value to record.
   */
  void Record(double value) {
    int bucket = bucket_scheme_->ToBucket(value);
    counts_[bucket] += 1.0f;
    count_ += 1;
  }

  /**
    Clear all recorded values.
   */
  void Clear() {
    std::fill(counts_.begin(), counts_.end(), 0.0f);
    count_ = 0;
  }

  /**
    Get the total number of values recorded.

    @return The count of recorded values.
   */
  int64_t GetCount() const { return count_; }

  /**
    Calculate a percentile from the histogram.

    @param quantile The desired quantile (0.0 to 1.0). For example, 0.95 for P95.
    @return The approximate percentile value, or NaN if no data has been recorded.
   */
  double GetPercentile(double quantile) const;

  /**
    Get the maximum value observed (approximated by highest non-empty bucket).

    @return The approximate maximum value, or NaN if no data has been recorded.
   */
  double GetMax() const;

  /**
    Get the mean value (approximated using bucket representative values).

    Values that exceeded max_value at record time are counted at max_value,
    so the mean is a lower bound when overflow values are present.

    @return The approximate mean value, or NaN if no data has been recorded.
   */
  double GetMean() const;

 private:
  const QuadraticBucketScheme *bucket_scheme_;
  std::vector<float> counts_;
  int64_t count_;
};

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

  // Moveable: bucket_scheme_ is a unique_ptr so its address is stable across
  // moves, keeping histogram_'s raw pointer to *bucket_scheme_ valid after the move.
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

  /**
    Flush the current histogram and return the old histogram.

    @return The old histogram containing accumulated data.
   */
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
