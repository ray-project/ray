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

#include "ray/stats/bucket_histogram_metric.h"

#include "ray/stats/quadratic_bucket_scheme.h"

namespace ray {
namespace stats {

BucketHistogramMetric::BucketHistogramMetric(const std::string &name,
                                             const std::string &description,
                                             const std::string &unit,
                                             double max_expected_value,
                                             int num_buckets,
                                             const std::vector<std::string> &tag_keys)
    // QuadraticBucketScheme validates num_buckets and max_expected_value.
    : histogram_(std::make_unique<Histogram>(
          name,
          description,
          unit,
          QuadraticBucketScheme(num_buckets, max_expected_value).ExplicitBounds(),
          tag_keys)) {}

}  // namespace stats
}  // namespace ray
