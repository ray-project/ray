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
#include <unordered_map>

#include "opencensus/stats/stats.h"
#include "opencensus/stats/stats_exporter.h"
#include "opencensus/tags/tag_key.h"
#include "ray/util/logging.h"

namespace ray {

namespace stats {

/// Include tag_defs.h to define tag items
#include "ray/stats/tag_defs.h"

/// StatsConfig per process.
/// Note that this is not thread-safe. Don't modify its internal values
/// outside stats::Init() or stats::Shutdown() method.
class StatsConfig final {
 public:
  static StatsConfig &instance();

  /// Get the current global tags.
  const TagsType &GetGlobalTags() const;

  /// Get whether or not stats are enabled.
  bool IsStatsDisabled() const;

  const absl::Duration &GetReportInterval() const;

  const absl::Duration &GetHarvestInterval() const;

  bool IsInitialized() const;

  ///
  /// Functions that should be used only inside stats::Init()
  /// NOTE: StatsConfig is not thread-safe. If you use these functions
  /// in multi threaded environment, it can cause problems.
  ///

  /// Set the stats have been initialized.
  void SetIsInitialized(bool initialized);
  /// Set the interval where metrics are harvetsed.
  void SetHarvestInterval(const absl::Duration interval);
  /// Set the interval where metrics are reported to data sinks.
  void SetReportInterval(const absl::Duration interval);
  /// Set if the stats are enabled in this process.
  void SetIsDisableStats(bool disable_stats);
  /// Set the global tags that will be appended to all metrics in this process.
  void SetGlobalTags(const TagsType &global_tags);

 private:
  StatsConfig() = default;
  ~StatsConfig() = default;
  StatsConfig(const StatsConfig &) = delete;
  StatsConfig &operator=(const StatsConfig &) = delete;

 private:
  TagsType global_tags_;
  /// If true, don't collect metrics in this process.
  bool is_stats_disabled_ = true;
  // Regular reporting interval for all reporters.
  absl::Duration report_interval_ = absl::Seconds(10);
  // Time interval for periodic aggregation.
  // Exporter may capture empty collection if harvest interval is longer than
  // report interval. So harvest interval is suggusted to be half of report
  // interval.
  absl::Duration harvest_interval_ = absl::Seconds(5);
  // Whether or not if the stats has been initialized.
  bool is_initialized_ = false;
};

/// A thin wrapper that wraps the `opencensus::tag::measure` for using it simply.
class Metric {
 public:
  Metric(const std::string &name, const std::string &description, const std::string &unit,
         const std::vector<opencensus::tags::TagKey> &tag_keys = {})
      : name_(name),
        description_(description),
        unit_(unit),
        tag_keys_(tag_keys),
        measure_(nullptr) {}

  virtual ~Metric() { opencensus::stats::StatsExporter::RemoveView(name_); }

  Metric &operator()() { return *this; }

  /// Get the name of this metric.
  std::string GetName() const { return name_; }

  /// Record the value for this metric.
  void Record(double value) { Record(value, TagsType{}); }

  /// Record the value for this metric.
  ///
  /// \param value The value that we record.
  /// \param tags The tag values that we want to record for this metric record.
  void Record(double value, const TagsType &tags);

  /// Record the value for this metric.
  ///
  /// \param value The value that we record.
  /// \param tags The map tag values that we want to record for this metric record.
  void Record(double value, std::unordered_map<std::string, std::string> &tags);

 protected:
  virtual void RegisterView() = 0;

 protected:
  std::string name_;
  std::string description_;
  std::string unit_;
  std::vector<opencensus::tags::TagKey> tag_keys_;
  std::unique_ptr<opencensus::stats::Measure<double>> measure_;

};  // class Metric

class Gauge : public Metric {
 public:
  Gauge(const std::string &name, const std::string &description, const std::string &unit,
        const std::vector<opencensus::tags::TagKey> &tag_keys = {})
      : Metric(name, description, unit, tag_keys) {}

 private:
  void RegisterView() override;

};  // class Gauge

class Histogram : public Metric {
 public:
  Histogram(const std::string &name, const std::string &description,
            const std::string &unit, const std::vector<double> boundaries,
            const std::vector<opencensus::tags::TagKey> &tag_keys = {})
      : Metric(name, description, unit, tag_keys), boundaries_(boundaries) {}

 private:
  void RegisterView() override;

 private:
  std::vector<double> boundaries_;

};  // class Histogram

class Count : public Metric {
 public:
  Count(const std::string &name, const std::string &description, const std::string &unit,
        const std::vector<opencensus::tags::TagKey> &tag_keys = {})
      : Metric(name, description, unit, tag_keys) {}

 private:
  void RegisterView() override;

};  // class Count

class Sum : public Metric {
 public:
  Sum(const std::string &name, const std::string &description, const std::string &unit,
      const std::vector<opencensus::tags::TagKey> &tag_keys = {})
      : Metric(name, description, unit, tag_keys) {}

 private:
  void RegisterView() override;

};  // class Sum

/// Raw metric view point for exporter.
struct MetricPoint {
  std::string metric_name;
  int64_t timestamp;
  double value;
  std::unordered_map<std::string, std::string> tags;
  const opencensus::stats::MeasureDescriptor &measure_descriptor;
};

}  // namespace stats

}  // namespace ray
