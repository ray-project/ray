// Copyright 2017 The Ray Authors.
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
#include <unordered_map>
#include <vector>

namespace ray {
class Metric {
 public:
  virtual ~Metric() = 0;

  /// Get the name of this metric.
  std::string GetName() const;

  /// Record the value for this metric.
  ///
  /// \param value The value that we record.
  /// \param tags The map tag values that we want to record for this metric record.
  void Record(double value, const std::unordered_map<std::string, std::string> &tags);

 protected:
  void *metric_ = nullptr;
};  // class Metric

class Gauge : public Metric {
 public:
  /// Gauges keep the last recorded value and drop everything before.
  /// Unlike counters, gauges can go up or down over time.
  ///
  /// This corresponds to Prometheus' gauge metric:
  /// https://prometheus.io/docs/concepts/metric_types/#gauge
  ///
  /// \param[in] name The Name of the metric.
  /// \param[in] description The Description of the metric.
  /// \param[in] unit The unit of the metric
  /// \param[in] tag_keys Tag keys of the metric.
  Gauge(const std::string &name,
        const std::string &description,
        const std::string &unit,
        const std::vector<std::string> &tag_keys = {});

  ///  Set the gauge to the given `value`.
  ///
  /// Tags passed in will take precedence over the metric's default tags.
  ///
  /// \param[in] value Value to set the gauge to.
  /// \param[in] tags Tags to set or override for this gauge.
  void Set(double value, const std::unordered_map<std::string, std::string> &tags);
};  // class Gauge

class Histogram : public Metric {
 public:
  /// Tracks the size and number of events in buckets.
  ///
  /// Histograms allow you to calculate aggregate quantiles
  /// such as 25, 50, 95, 99 percentile latency for an RPC.
  ///
  /// This corresponds to Prometheus' histogram metric:
  /// https://prometheus.io/docs/concepts/metric_types/#histogram
  ///
  /// \param[in] name The Name of the metric.
  /// \param[in] description The Description of the metric.
  /// \param[in] unit The unit of the metric
  /// \param[in] boundaries The Boundaries of histogram buckets.
  /// \param[in] tag_keys Tag keys of the metric.
  Histogram(const std::string &name,
            const std::string &description,
            const std::string &unit,
            const std::vector<double> boundaries,
            const std::vector<std::string> &tag_keys = {});

  /// Observe a given `value` and add it to the appropriate bucket.
  ///
  /// Tags passed in will take precedence over the metric's default tags.
  ///
  /// \param[in] value The value that we record.
  /// \param[in] tags The map tag values that we want to record
  void Observe(double value, const std::unordered_map<std::string, std::string> &Tags);
};  // class Histogram

class Counter : public Metric {
 public:
  /// A cumulative metric that is monotonically increasing.
  ///
  /// This corresponds to Prometheus' counter metric:
  /// https://prometheus.io/docs/concepts/metric_types/#counter
  ///
  /// \param[in] name The Name of the metric.
  /// \param[in] description The Description of the metric.
  /// \param[in] unit The unit of the metric
  /// \param[in] tag_keys Tag keys of the metric.
  Counter(const std::string &name,
          const std::string &description,
          const std::string &unit,
          const std::vector<std::string> &tag_keys = {});

  /// Increment the counter by `value` (defaults to 1).
  ///
  /// Tags passed in will take precedence over the metric's default tags.
  ///
  /// \param[in] value Value to increment the counter by (default=1).
  /// \param[in] tags The map tag values that we want to record
  void Inc(double value, const std::unordered_map<std::string, std::string> &tags);
};  // class Counter

class Sum : public Metric {
 public:
  /// A sum up of the metric points.
  ///
  /// \param[in] name The Name of the metric.
  /// \param[in] description The Description of the metric.
  /// \param[in] unit The unit of the metric
  /// \param[in] tag_keys Tag keys of the metric.
  Sum(const std::string &name,
      const std::string &description,
      const std::string &unit,
      const std::vector<std::string> &tag_keys = {});
};  // class Sum

}  // namespace ray
