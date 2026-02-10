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

#include <cctype>
#include <cstdint>
#include <functional>
#include <memory>
#include <regex>
#include <utility>

#include "absl/container/flat_hash_map.h"
#include "opencensus/stats/stats.h"
#include "opencensus/tags/tag_key.h"
#include "ray/common/ray_config.h"
#include "ray/observability/metric_interface.h"
#include "ray/observability/open_telemetry_metric_recorder.h"
#include "ray/util/logging.h"

namespace ray {

namespace stats {

using OpenTelemetryMetricRecorder = ray::observability::OpenTelemetryMetricRecorder;

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
  /// Add the initializer
  void AddInitializer(std::function<void()> func) {
    initializers_.push_back(std::move(func));
  }
  std::vector<std::function<void()>> PopInitializers() {
    return std::move(initializers_);
  }

  ~StatsConfig() = default;
  StatsConfig(const StatsConfig &) = delete;
  StatsConfig &operator=(const StatsConfig &) = delete;

 private:
  StatsConfig() = default;

  TagsType global_tags_;
  /// If true, don't collect metrics in this process.
  bool is_stats_disabled_ = true;
  // Regular reporting interval for all reporters.
  absl::Duration report_interval_ = absl::Milliseconds(10000);
  // Time interval for periodic aggregation.
  // Exporter may capture empty collection if harvest interval is longer than
  // report interval. So harvest interval is suggusted to be half of report
  // interval.
  absl::Duration harvest_interval_ = absl::Milliseconds(5000);
  // Whether or not if the stats has been initialized.
  bool is_initialized_ = false;
  std::vector<std::function<void()>> initializers_;
};

/// A thin wrapper that wraps the `opencensus::tag::measure` for using it simply.
class Metric : public observability::MetricInterface {
 public:
  Metric(const std::string &name,
         std::string description,
         std::string unit,
         const std::vector<std::string> &tag_keys = {});

  ~Metric() = default;

  Metric &operator()() { return *this; }

  static const std::regex &GetMetricNameRegex();

  /// Get the name of this metric.
  const std::string &GetName() const { return name_; }

  /// Record the value for this metric.
  void Record(double value) override { Record(value, TagsType{}); }

  /// Record the value for this metric.
  ///
  /// \param value The value that we record.
  /// \param tags The tag values that we want to record for this metric record.
  void Record(double value, TagsType tags) override;

  /// Record the value for this metric.
  ///
  /// \param value The value that we record.
  /// \param tags The map tag values that we want to record for this metric record.
  void Record(double value,
              std::vector<std::pair<std::string_view, std::string>> tags) override;

  /// Our version of Cython doesn't support string_view (later versions do), so we need to
  /// have this for it.
  void RecordForCython(double value,
                       std::vector<std::pair<std::string, std::string>> tags);

 protected:
  virtual void RegisterView() = 0;
  virtual void RegisterOpenTelemetryMetric() = 0;

 protected:
  std::string name_;
  std::string description_;
  std::string unit_;
  std::vector<opencensus::tags::TagKey> tag_keys_;
  std::unique_ptr<opencensus::stats::Measure<double>> measure_;

 private:
  const std::regex &name_regex_;

  // For making sure thread-safe to all of metric registrations.
  inline static absl::Mutex registration_mutex_;
};  // class Metric

class Gauge : public Metric {
 public:
  Gauge(const std::string &name,
        const std::string &description,
        const std::string &unit,
        const std::vector<std::string> &tag_keys = {})
      : Metric(name, description, unit, tag_keys) {
    if (::RayConfig::instance().enable_open_telemetry()) {
      RegisterOpenTelemetryMetric();
    }
  }

 private:
  void RegisterView() override;
  void RegisterOpenTelemetryMetric() override;

};  // class Gauge

class Histogram : public Metric {
 public:
  Histogram(const std::string &name,
            const std::string &description,
            const std::string &unit,
            const std::vector<double> &boundaries,
            const std::vector<std::string> &tag_keys = {})
      : Metric(name, description, unit, tag_keys), boundaries_(boundaries) {
    if (::RayConfig::instance().enable_open_telemetry()) {
      RegisterOpenTelemetryMetric();
    }
  }

 private:
  void RegisterView() override;
  void RegisterOpenTelemetryMetric() override;

 private:
  std::vector<double> boundaries_;

};  // class Histogram

class Count : public Metric {
 public:
  Count(const std::string &name,
        const std::string &description,
        const std::string &unit,
        const std::vector<std::string> &tag_keys = {})
      : Metric(name, description, unit, tag_keys) {
    if (::RayConfig::instance().enable_open_telemetry()) {
      RegisterOpenTelemetryMetric();
    }
  }

 private:
  void RegisterView() override;
  void RegisterOpenTelemetryMetric() override;

};  // class Count

class Sum : public Metric {
 public:
  Sum(const std::string &name,
      const std::string &description,
      const std::string &unit,
      const std::vector<std::string> &tag_keys = {})
      : Metric(name, description, unit, tag_keys) {
    if (::RayConfig::instance().enable_open_telemetry()) {
      RegisterOpenTelemetryMetric();
    }
  }

 private:
  void RegisterView() override;
  void RegisterOpenTelemetryMetric() override;

};  // class Sum

enum StatsType : uint8_t { COUNT, SUM, GAUGE, HISTOGRAM };

namespace internal {
void RegisterAsView(opencensus::stats::ViewDescriptor view_descriptor,
                    const std::vector<opencensus::tags::TagKey> &keys);
template <StatsType T>
struct StatsTypeMap {
  static constexpr const char *val = "_void";
};

template <>
struct StatsTypeMap<COUNT> {
  static opencensus::stats::Aggregation Aggregation(const std::vector<double> &) {
    return opencensus::stats::Aggregation::Count();
  }
  static constexpr const char *val = "_cnt";
};

template <>
struct StatsTypeMap<SUM> {
  static opencensus::stats::Aggregation Aggregation(const std::vector<double> &) {
    return opencensus::stats::Aggregation::Sum();
  }
  static constexpr const char *val = "_sum";
};

template <>
struct StatsTypeMap<GAUGE> {
  static opencensus::stats::Aggregation Aggregation(const std::vector<double> &) {
    return opencensus::stats::Aggregation::LastValue();
  }
  static constexpr const char *val = "_gauge";
};

template <>
struct StatsTypeMap<HISTOGRAM> {
  static opencensus::stats::Aggregation Aggregation(const std::vector<double> &buckets) {
    return opencensus::stats::Aggregation::Distribution(
        opencensus::stats::BucketBoundaries::Explicit(buckets));
  }
  static constexpr const char *val = "_dist";
};

template <StatsType T>
void RegisterView(const std::string &name,
                  const std::string &description,
                  const std::vector<opencensus::tags::TagKey> &tag_keys,
                  const std::vector<double> &buckets) {
  if (!::RayConfig::instance().enable_open_telemetry()) {
    // OpenTelemetry is not enabled, register the view as an OpenCensus view.
    using I = StatsTypeMap<T>;
    auto view_descriptor = opencensus::stats::ViewDescriptor()
                               .set_name(name + I::val)
                               .set_description(description)
                               .set_measure(name)
                               .set_aggregation(I::Aggregation(buckets));
    internal::RegisterAsView(view_descriptor, tag_keys);
    return;
  }
  if (T == GAUGE) {
    OpenTelemetryMetricRecorder::GetInstance().RegisterGaugeMetric(name, description);
  } else if (T == COUNT) {
    OpenTelemetryMetricRecorder::GetInstance().RegisterCounterMetric(name, description);
  } else if (T == SUM) {
    OpenTelemetryMetricRecorder::GetInstance().RegisterSumMetric(name, description);
  } else if (T == HISTOGRAM) {
    OpenTelemetryMetricRecorder::GetInstance().RegisterHistogramMetric(
        name, description, buckets);
  } else {
    RAY_CHECK(false) << "Unknown stats type: " << static_cast<int>(T);
  }
}

template <typename T = void>
void RegisterViewWithTagList(const std::string &name,
                             const std::string &description,
                             const std::vector<opencensus::tags::TagKey> &tag_keys,
                             const std::vector<double> &buckets) {
  static_assert(std::is_same_v<T, void>);
}

template <StatsType T, StatsType... Ts>
void RegisterViewWithTagList(const std::string &name,
                             const std::string &description,
                             const std::vector<opencensus::tags::TagKey> &tag_keys,
                             const std::vector<double> &buckets) {
  RegisterView<T>(name, description, tag_keys, buckets);
  RegisterViewWithTagList<Ts...>(name, description, tag_keys, buckets);
}

}  // namespace internal

}  // namespace stats

}  // namespace ray
