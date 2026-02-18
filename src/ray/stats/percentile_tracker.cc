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

#include "ray/stats/percentile_tracker.h"

#include <cmath>

namespace ray {
namespace stats {

// QuadraticBucketScheme

double QuadraticBucketScheme::FromBucket(int bucket) const {
  if (bucket >= num_buckets_ - 1) {
    return max_value_;
  }
  double unscaled = (bucket * (bucket + 1.0)) / 2.0;
  return unscaled * scale_;
}

// BucketHistogram

double BucketHistogram::GetPercentile(double quantile) const {
  if (count_ == 0) {
    return std::nan("");
  }
  const double inv_count = 1.0 / static_cast<double>(count_);
  double sum = 0.0;
  for (size_t i = 0; i < counts_.size(); i++) {
    sum += static_cast<double>(counts_[i]);
    if (sum * inv_count > quantile) {
      return bucket_scheme_->FromBucket(i);
    }
  }
  return bucket_scheme_->FromBucket(counts_.size() - 1);
}

double BucketHistogram::GetMax() const {
  if (count_ == 0) {
    return std::nan("");
  }
  for (int i = static_cast<int>(counts_.size()) - 1; i >= 0; i--) {
    if (counts_[i] > 0) {
      return bucket_scheme_->FromBucket(i);
    }
  }
  return std::nan("");
}

double BucketHistogram::GetMean() const {
  if (count_ == 0) {
    return std::nan("");
  }
  double sum = 0.0;
  for (size_t i = 0; i < counts_.size(); i++) {
    if (counts_[i] > 0) {
      sum += static_cast<double>(counts_[i]) * bucket_scheme_->FromBucket(i);
    }
  }
  return sum / static_cast<double>(count_);
}

// PercentileTracker

/**
  Flush the current histogram and return the old histogram.

  In order to keep this light, this amounts to a pointer swap. A pointer
  to the old histogram is returned and a new histogram is constructed and
  saved on this instance.

  @return The old histogram containing accumulated data.
 */
std::unique_ptr<BucketHistogram> PercentileTracker::GetAndFlushHistogram() {
  auto old_histogram = std::move(histogram_);
  histogram_ = std::make_unique<BucketHistogram>(bucket_scheme_.get());
  return old_histogram;
}

}  // namespace stats
}  // namespace ray
