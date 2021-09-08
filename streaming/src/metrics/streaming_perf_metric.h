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
#include "config/streaming_config.h"

namespace ray {
namespace streaming {
#define METRIC_GROUP_JOIN(a, b, c) (a + "." + b + "." + c)

class StreamingReporterInterface {
 public:
  virtual ~StreamingReporterInterface() = default;
  virtual bool Start(const StreamingMetricsConfig &conf) = 0;

  virtual void Shutdown() = 0;

  virtual void UpdateCounter(const std::string &domain, const std::string &group_name,
                             const std::string &short_name, double value) = 0;

  virtual void UpdateGauge(const std::string &domain, const std::string &group_name,
                           const std::string &short_name, double value,
                           bool is_reset) = 0;

  virtual void UpdateHistogram(const std::string &domain, const std::string &group_name,
                               const std::string &short_name, double value,
                               double min_value, double max_value) = 0;

  virtual void UpdateCounter(const std::string &metric_name,
                             const std::unordered_map<std::string, std::string> &tags,
                             double value) = 0;

  virtual void UpdateGauge(const std::string &metric_name,
                           const std::unordered_map<std::string, std::string> &tags,
                           double value, bool is_rest) = 0;

  virtual void UpdateHistogram(const std::string &metric_name,
                               const std::unordered_map<std::string, std::string> &tags,
                               double value, double min_value, double max_value) = 0;

  virtual void UpdateQPS(const std::string &metric_name,
                         const std::unordered_map<std::string, std::string> &tags,
                         double value) = 0;
};

/// Streaming perf is a reporter instance based multiple backend.
/// Other modules can report gauge/histogram/counter/qps measurement to meteric server
/// side.
class StreamingReporter : public StreamingReporterInterface {
 public:
  StreamingReporter(){};
  virtual ~StreamingReporter();
  bool Start(const StreamingMetricsConfig &conf) override;
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

 private:
  std::unique_ptr<StreamingReporterInterface> impl_;
};
}  // namespace streaming

}  // namespace ray
