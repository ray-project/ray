// Copyright 2021 The Ray Authors.
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

#include <mutex>

#include "ray/stats/metric.h"
#include "streaming_perf_metric.h"

namespace ray {
namespace streaming {

class StatsReporter : public StreamingReporterInterface {
 public:
  virtual ~StatsReporter();

  bool Start(const StreamingMetricsConfig &conf) override;

  bool Start(const std::string &json_string);

  void Shutdown() override;

  void UpdateCounter(const std::string &domain, const std::string &group_name,
                     const std::string &short_name, double value) override;

  void UpdateGauge(const std::string &domain, const std::string &group_name,
                   const std::string &short_name, double value,
                   bool is_reset = true) override;

  void UpdateHistogram(const std::string &domain, const std::string &group_name,
                       const std::string &short_name, double value, double min_value,
                       double max_value) override;

  void UpdateCounter(const std::string &metric_name,
                     const std::unordered_map<std::string, std::string> &tags,
                     double value) override;

  void UpdateGauge(const std::string &metric_name,
                   const std::unordered_map<std::string, std::string> &tags, double value,
                   bool is_rest = true) override;

  void UpdateHistogram(const std::string &metric_name,
                       const std::unordered_map<std::string, std::string> &tags,
                       double value, double min_value, double max_value) override;

  void UpdateQPS(const std::string &metric_name,
                 const std::unordered_map<std::string, std::string> &tags,
                 double value) override;

 protected:
  std::shared_ptr<ray::stats::Metric> GetMetricByName(const std::string &metric_name);
  void MetricRegister(const std::string &metric_name,
                      std::shared_ptr<ray::stats::Metric> metric);
  void UnregisterAllMetrics();

 private:
  inline std::unordered_map<std::string, std::string> MergeGlobalTags(
      const std::unordered_map<std::string, std::string> &tags) {
    std::unordered_map<std::string, std::string> merged_tags;
    merged_tags.insert(global_tags_.begin(), global_tags_.end());
    for (auto &item : tags) {
      merged_tags.emplace(item.first, item.second);
    }
    return merged_tags;
  }

 private:
  std::mutex metric_mutex_;
  std::unordered_map<std::string, std::shared_ptr<ray::stats::Metric>> metric_map_;
  std::unordered_map<std::string, std::string> global_tags_;
  std::vector<stats::TagKeyType> global_tag_key_list_;
  std::string service_name_;
};

}  // namespace streaming
}  // namespace ray
