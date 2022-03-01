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

#include <ctype.h>

#include <functional>
#include <memory>
#include <tuple>
#include <unordered_map>

#include "gtest/gtest_prod.h"
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
  /// Add the initializer
  void AddInitializer(std::function<void()> func) {
    initializers_.push_back(std::move(func));
  }
  std::vector<std::function<void()>> PopInitializers() {
    return std::move(initializers_);
  }

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
class Metric {
 public:
  Metric(const std::string &name, const std::string &description, const std::string &unit,
         const std::vector<opencensus::tags::TagKey> &tag_keys = {})
      : name_(name),
        description_(description),
        unit_(unit),
        tag_keys_(tag_keys),
        measure_(nullptr) {}

  virtual ~Metric();

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
  void Record(double value, const std::unordered_map<std::string, std::string> &tags);

 protected:
  virtual void RegisterView() = 0;

 protected:
  std::string name_;
  std::string description_;
  std::string unit_;
  std::vector<opencensus::tags::TagKey> tag_keys_;
  std::unique_ptr<opencensus::stats::Measure<double>> measure_;

  // For making sure thread-safe to all of metric registrations.
  static absl::Mutex registration_mutex_;

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

enum StatsType : int { COUNT, SUM, GAUGE, HISTOGRAM };

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
void RegisterView(const std::string &name, const std::string &description,
                  const std::vector<opencensus::tags::TagKey> &tag_keys,
                  const std::vector<double> &buckets) {
  using I = StatsTypeMap<T>;
  auto view_descriptor = opencensus::stats::ViewDescriptor()
                             .set_name(name + I::val)
                             .set_description(description)
                             .set_measure(name)
                             .set_aggregation(I::Aggregation(buckets));
  internal::RegisterAsView(view_descriptor, tag_keys);
}

template <typename T = void>
void RegisterViewWithTagList(const std::string &name, const std::string &description,
                             const std::vector<opencensus::tags::TagKey> &tag_keys,
                             const std::vector<double> &buckets) {
  static_assert(std::is_same_v<T, void>);
}

template <StatsType T, StatsType... Ts>
void RegisterViewWithTagList(const std::string &name, const std::string &description,
                             const std::vector<opencensus::tags::TagKey> &tag_keys,
                             const std::vector<double> &buckets) {
  RegisterView<T>(name, description, tag_keys, buckets);
  RegisterViewWithTagList<Ts...>(name, description, tag_keys, buckets);
}

inline std::vector<opencensus::tags::TagKey> convert_tags(
    const std::vector<std::string> &names) {
  std::vector<opencensus::tags::TagKey> ret;
  for (auto &n : names) {
    ret.push_back(TagKeyType::Register(n));
  }
  return ret;
}

/*
  This is a helper class to define a metrics. With this class
  we'll be able to define a multi-view-single-measure metric for
  efficiency (TODO Fix the bug in backend to make it work).
  TODO Remove old metrics code.
*/
class Stats {
  using Measure = opencensus::stats::Measure<double>;

 public:
  /// Define a metric.
  /// \param measure The name for the metric
  /// \description The description for the metric
  /// \register_func The function to register the metric
  Stats(const std::string &measure, const std::string &description,
        std::vector<std::string> tag_keys, std::vector<double> buckets,
        std::function<void(const std::string &, const std::string,
                           const std::vector<opencensus::tags::TagKey>,
                           const std::vector<double> &buckets)>
            register_func)
      : tag_keys_(convert_tags(tag_keys)) {
    auto stats_init = [register_func, measure, description, buckets, this]() {
      measure_ = std::make_unique<Measure>(Measure::Register(measure, description, ""));
      register_func(measure, description, tag_keys_, buckets);
    };

    if (StatsConfig::instance().IsInitialized()) {
      stats_init();
    } else {
      StatsConfig::instance().AddInitializer(stats_init);
    }
  }

  /// Record a value
  /// \param val The value to record
  void Record(double val) { Record(val, std::unordered_map<std::string, std::string>()); }

  /// Record a value
  /// \param val The value to record
  /// \param tag_val The tag value. This method will assume we only have one tag for
  /// this metric.
  void Record(double val, std::string tag_val) {
    RAY_CHECK(tag_keys_.size() == 1);
    if (StatsConfig::instance().IsStatsDisabled() || !measure_) {
      return;
    }
    TagsType combined_tags = StatsConfig::instance().GetGlobalTags();
    CheckPrintableChar(tag_val);
    combined_tags.emplace_back(tag_keys_[0], std::move(tag_val));
    opencensus::stats::Record({{*measure_, val}}, std::move(combined_tags));
  }

  /// Record a value
  /// \param val The value to record
  /// \param tags The tags for this value
  void Record(double val, std::unordered_map<std::string, std::string> tags) {
    if (StatsConfig::instance().IsStatsDisabled() || !measure_) {
      return;
    }
    TagsType combined_tags = StatsConfig::instance().GetGlobalTags();
    for (auto &[tag_key, tag_val] : tags) {
      CheckPrintableChar(tag_val);
      combined_tags.emplace_back(TagKeyType::Register(tag_key), std::move(tag_val));
    }
    opencensus::stats::Record({{*measure_, val}}, std::move(combined_tags));
  }

 private:
  void CheckPrintableChar(const std::string &val) {
#ifndef NDEBUG
    // In debug build, verify val is printable.
    for (auto c : val) {
      RAY_CHECK(isprint(c)) << "Found unprintable character code " << static_cast<int>(c)
                            << " in " << val;
    }
#endif  // NDEBUG
  }

  const std::vector<opencensus::tags::TagKey> tag_keys_;
  std::unique_ptr<opencensus::stats::Measure<double>> measure_;
};

}  // namespace internal

}  // namespace stats

}  // namespace ray

#define DECLARE_stats(name) extern ray::stats::internal::Stats STATS_##name

// STATS_DEPAREN will remove () for it's parameter
// For example
//   STATS_DEPAREN((a, b, c))
// will result
//   a, b, c
#define STATS_DEPAREN(X) STATS_ESC(STATS_ISH X)
#define STATS_ISH(...) ISH __VA_ARGS__
#define STATS_ESC(...) STATS_ESC_(__VA_ARGS__)
#define STATS_ESC_(...) STATS_VAN##__VA_ARGS__
#define STATS_VANISH

/*
  Syntax sugar to define a metrics:
      DEFINE_stats(name,
        desctiption,
        (tag1, tag2, ...),
        (bucket1, bucket2, ...),
        type1,
        type2)
  Later, it can be used by STATS_name.record(val, tags).

  Some examples:
      DEFINE_stats(
          async_pool_req_execution_time_ms,
          "Async pool execution time",
          ("Method"),
          (), ray::stats::GAUGE);
      STATS_async_pool_req_execution_time_ms.record(1, "method");
*/
#define DEFINE_stats(name, description, tags, buckets, ...)                \
  ray::stats::internal::Stats STATS_##name(                                \
      #name, description, {STATS_DEPAREN(tags)}, {STATS_DEPAREN(buckets)}, \
      ray::stats::internal::RegisterViewWithTagList<__VA_ARGS__>)
