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

namespace ray {
namespace stats {

int QuadraticBucketScheme::ToBucket(double value) const {
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

}  // namespace stats
}  // namespace ray
