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

#include <algorithm>
#include <cstdint>
#include <vector>

#include "ray/stats/quadratic_bucket_scheme.h"

namespace ray {
namespace stats {

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
  void Record(double value);

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

}  // namespace stats
}  // namespace ray
