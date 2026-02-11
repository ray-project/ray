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

#include "ray/observability/metric_interface.h"
#include "ray/stats/percentile_metric.h"

namespace ray {
namespace stats {

/// Wrapper metric that records to multiple metric implementations simultaneously
/// for A/B/C testing and comparison purposes.
///
/// This class implements MetricInterface and delegates Record() calls to:
/// 1. Original histogram (baseline with explicit buckets)
/// 2. Percentile-based metric (client-side percentile tracking)
/// 3. Exponential histogram (OpenTelemetry exponential buckets)
///
/// This allows comparing the accuracy, cardinality, and performance of different
/// approaches in production without code changes.
class ComparisonMetric : public observability::MetricInterface {
 public:
  /// Create a comparison metric that records to all three implementations.
  /// Takes ownership of the histogram and exponential metrics.
  ///
  /// \param histogram Original histogram metric (baseline) - transfers ownership
  /// \param percentile Percentile-based metric (emits 5 gauges)
  /// \param exponential Exponential histogram metric - transfers ownership
  ComparisonMetric(std::unique_ptr<observability::MetricInterface> histogram,
                   std::shared_ptr<PercentileMetric> percentile,
                   std::unique_ptr<observability::MetricInterface> exponential)
      : histogram_(std::move(histogram)),
        percentile_(percentile),
        exponential_(std::move(exponential)) {}

  /// Record a value to all three metric implementations.
  void Record(double value) override {
    if (histogram_) {
      histogram_->Record(value);
    }
    if (percentile_) {
      percentile_->Record(value);
    }
    if (exponential_) {
      exponential_->Record(value);
    }
  }

  /// Record a value with tags to all three metric implementations.
  /// Note: PercentileMetric doesn't support tags, so tags are ignored for that
  /// implementation.
  void Record(double value, TagsType tags) override {
    if (histogram_) {
      histogram_->Record(value, tags);
    }
    if (percentile_) {
      // PercentileMetric doesn't support tags, record without them
      percentile_->Record(value);
    }
    if (exponential_) {
      exponential_->Record(value, tags);
    }
  }

  /// Record a value with tags (vector variant).
  void Record(double value,
              std::vector<std::pair<std::string_view, std::string>> tags) override {
    if (histogram_) {
      histogram_->Record(value, tags);
    }
    if (percentile_) {
      // PercentileMetric doesn't support tags, record without them
      percentile_->Record(value);
    }
    if (exponential_) {
      exponential_->Record(value, tags);
    }
  }

 private:
  std::unique_ptr<observability::MetricInterface> histogram_;
  std::shared_ptr<PercentileMetric> percentile_;
  std::unique_ptr<observability::MetricInterface> exponential_;
};

}  // namespace stats
}  // namespace ray
