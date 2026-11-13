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

#include "ray/stats/quadratic_bucket_scheme.h"

#include <algorithm>
#include <cmath>
#include <limits>
#include <vector>

namespace ray {
namespace stats {

double QuadraticBucketScheme::UpperBound(int bucket) const {
  RAY_CHECK(bucket >= 0 && bucket < num_buckets_)
      << "bucket must be in [0, " << num_buckets_ << "), got " << bucket;
  if (bucket == num_buckets_ - 1) {
    return std::numeric_limits<double>::infinity();
  }
  if (bucket == num_buckets_ - 2) {
    // Pin the last finite bound to exactly max_value to avoid floating point drift.
    return max_value_;
  }
  return scale_ * (bucket + 1) * (bucket + 2) / 2.0;
}

std::vector<double> QuadraticBucketScheme::ExplicitBounds() const {
  std::vector<double> bounds;
  bounds.reserve(num_buckets_ - 1);
  for (int b = 0; b < num_buckets_ - 1; b++) {
    bounds.push_back(UpperBound(b));
  }
  return bounds;
}

int QuadraticBucketScheme::ToBucket(double value) const {
  if (value <= UpperBound(0)) {
    return 0;
  }
  if (value > max_value_) {
    return num_buckets_ - 1;
  }
  const double scaled = value / scale_;
  int bucket = static_cast<int>(std::ceil(std::sqrt(2.0 * scaled + 0.25) - 1.5));
  bucket = std::clamp(bucket, 1, num_buckets_ - 2);
  // The closed form can be off by one from floating point error at a boundary, so
  // settle the result against the exact bounds that are exported.
  while (bucket > 1 && value <= UpperBound(bucket - 1)) {
    bucket--;
  }
  while (bucket < num_buckets_ - 2 && value > UpperBound(bucket)) {
    bucket++;
  }
  return bucket;
}

}  // namespace stats
}  // namespace ray
