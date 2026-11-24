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

#pragma once

#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "ray/observability/metric_interface.h"
#include "ray/stats/metric.h"

namespace ray {
namespace stats {

/**
  @class BucketHistogramMetric

  Histogram metric whose bucket boundaries follow the quadratic scheme defined by
  QuadraticBucketScheme (see ray/stats/quadratic_bucket_scheme.h).
 */
class BucketHistogramMetric : public ray::observability::MetricInterface {
 public:
  /**
    Construct a quadratic-bucket histogram metric.

    @param name Metric name.
    @param description Metric description.
    @param unit Unit of measurement (e.g., "ms").
    @param max_expected_value Maximum expected value for histogram bucketing.
    Values above this go to the overflow bucket. Must be positive.
    @param num_buckets Number of buckets (including the overflow bucket) to
    adjust the resolution of the histogram. Must be >= 2.
    @param tag_keys Tag keys recorded with each sample.
   */
  BucketHistogramMetric(const std::string &name,
                        const std::string &description,
                        const std::string &unit,
                        double max_expected_value,
                        int num_buckets = 256,
                        const std::vector<std::string> &tag_keys = {});

  void Record(double value) override { histogram_->Record(value); }

  void Record(double value, stats::TagsType tags) override {
    histogram_->Record(value, std::move(tags));
  }

  void Record(double value,
              std::vector<std::pair<std::string_view, std::string>> tags) override {
    histogram_->Record(value, std::move(tags));
  }

 private:
  std::unique_ptr<Histogram> histogram_;
};

}  // namespace stats
}  // namespace ray
