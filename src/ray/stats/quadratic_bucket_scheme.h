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
  Each bucket includes its upper boundary and excludes its lower boundary,
  as Prometheus histograms do (see ToBucket).

  This provides fine granularity where most data is (low latencies) and coarse
  granularity for rare outliers (high latencies).

  N.B. The last bucket in the scheme should be viewed as the 'to infinity' bucket.
  Recordings which land in this bucket should be viewed as 'noteworthy' (or broken)
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
    Get the inclusive upper bound of a bucket.

    @param bucket The bucket index, in [0, num_buckets - 1].
    @return The inclusive upper bound of the bucket.
   */
  double UpperBound(int bucket) const;

  /**
    Get the finite bucket upper bounds, for an explicit-bucket histogram.

    @return UpperBound(b) for b in [0, num_buckets - 2]. Explicit-bucket
    histograms (OpenTelemetry, Prometheus) add an implicit +inf bucket after
    the last bound, which serves as this scheme's overflow bucket, so a value
    lands in the same bucket there as in ToBucket.
   */
  std::vector<double> ExplicitBounds() const;

  /**
    Map a value to its corresponding bucket index.

    Buckets include their upper bound and exclude their lower bound, i.e. bucket
    b holds values in (UpperBound(b - 1), UpperBound(b)]. This matches
    Prometheus `le` buckets and OpenTelemetry explicit bucket histograms, so a
    value lands in the same bucket here as in the exported histogram.

    @param value The value to map to a bucket.
    @return The bucket index for this value, clamped to [0, num_buckets - 1].
   */
  int ToBucket(double value) const;

  /**
    Get the representative value for a bucket index.

    For buckets 0 through num_buckets-2 this returns the midpoint of the
    bucket, i.e. the average of its lower and upper boundaries. Using the
    midpoint gives an unbiased estimate (assuming uniform distribution within
    each bucket) and maintains strict monotonicity across all buckets.
    For the last bucket (overflow bucket) this returns max_value.

    @param bucket The bucket index.
    @return The representative value for this bucket.
   */
  double FromBucket(int bucket) const {
    if (bucket >= num_buckets_ - 1) {
      return max_value_;
    }
    double unscaled = ((bucket + 1.0) * (bucket + 1.0)) / 2.0;
    return unscaled * scale_;
  }

 private:
  int num_buckets_;
  double max_value_;
  double scale_;
};

}  // namespace stats
}  // namespace ray
