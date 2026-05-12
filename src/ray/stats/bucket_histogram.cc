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

#include "ray/stats/bucket_histogram.h"

#include <cmath>

namespace ray {
namespace stats {

void BucketHistogram::Record(double value) {
  int bucket = bucket_scheme_->ToBucket(value);
  counts_[bucket] += 1.0f;
  count_ += 1;
}

double BucketHistogram::GetPercentile(double quantile) const {
  if (count_ == 0) {
    return std::nan("");
  }
  const double inv_count = 1.0 / static_cast<double>(count_);
  double sum = 0.0;
  for (size_t i = 0; i < counts_.size(); i++) {
    sum += static_cast<double>(counts_[i]);
    if (sum * inv_count >= quantile) {
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
    sum += static_cast<double>(counts_[i]) * bucket_scheme_->FromBucket(i);
  }
  return sum / static_cast<double>(count_);
}

}  // namespace stats
}  // namespace ray
