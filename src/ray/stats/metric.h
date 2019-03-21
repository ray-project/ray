#ifndef RAY_STATS_METRIC_H_
#define RAY_STATS_METRIC_H_

#include "opencensus/exporters/stats/prometheus/prometheus_exporter.h"
#include "prometheus/exposer.h"
#include "opencensus/tags/tag_key.h"
#include "opencensus/stats/stats.h"

namespace ray {

namespace stats {

/// Include tag_defs.h to define tag items
#include "tag_defs.h"

/// The helper function for registering a view.
static void RegisterAsView(opencensus::stats::ViewDescriptor view_descriptor,
                           const std::vector<opencensus::tags::TagKey>& keys) {
  // Register global keys.
  for (const auto &tag : GlobalTags) {
    view_descriptor = view_descriptor.add_column(tag.first);
  }

  // Register custom keys.
  for (const auto &key : keys) {
    view_descriptor = view_descriptor.add_column(key);
  }

  opencensus::stats::View view(view_descriptor);
  view_descriptor.RegisterForExport();
}

/// A thin wrapper that wraps the `opencensus::tag::measure` for using it simply.
class Metric final {

 public:
  ~Metric() = default;

  Metric& operator()() {
    return *this;
  }

  /// Create a gauge type metric and return it.
  static Metric MakeGauge(const std::string &name,
                          const std::string &description,
                          const std::string &unit,
                          const std::vector<opencensus::tags::TagKey>& keys = {}) {
    auto metric = Metric(name, description, unit);

    opencensus::stats::ViewDescriptor view_descriptor =
          opencensus::stats::ViewDescriptor().set_name(std::string("raylet/") + name)
              .set_description(description)
              .set_measure(metric.GetName())
              .set_aggregation(opencensus::stats::Aggregation::LastValue());

    RegisterAsView(view_descriptor, keys);
    return metric;
  }

  /// Create a histogram type metric and return it.
  static Metric MakeHistogram(const std::string &name,
                              const std::string &description,
                              const std::string &unit,
                              const std::vector<double> boundaries,
                              const std::vector<opencensus::tags::TagKey>& keys = {}) {
    auto metric = Metric(name, description, unit); 
    opencensus::stats::ViewDescriptor view_descriptor =
          opencensus::stats::ViewDescriptor().set_name(name)
              .set_description(description)
              .set_measure(metric.GetName())
              .set_aggregation(opencensus::stats::Aggregation::Distribution(
                opencensus::stats::BucketBoundaries::Explicit(boundaries)));

    RegisterAsView(view_descriptor, keys);
    return metric;
  }

  /// Create a count type metric and return it.
  static Metric MakeCount(const std::string &name,
                          const std::string &description,
                          const std::string &unit,
                          const std::vector<opencensus::tags::TagKey>& keys = {}) {
    auto metric = Metric(name, description, unit);
    opencensus::stats::ViewDescriptor view_descriptor =
        opencensus::stats::ViewDescriptor().set_name(name)
            .set_description(description)
            .set_measure(metric.GetName())
            .set_aggregation(opencensus::stats::Aggregation::Count());

    RegisterAsView(view_descriptor, keys);
    return metric;
  }

    /// Create a sum type metric and return it.
    static Metric MakeSum(const std::string &name,
                            const std::string &description,
                            const std::string &unit,
                            const std::vector<opencensus::tags::TagKey>& keys = {}) {
      auto metric = Metric(name, description, unit);
      opencensus::stats::ViewDescriptor view_descriptor =
          opencensus::stats::ViewDescriptor().set_name(name)
              .set_description(description)
              .set_measure(metric.GetName())
              .set_aggregation(opencensus::stats::Aggregation::Sum());

      RegisterAsView(view_descriptor, keys);
      return metric;
    }

  /// Get the name of this metric.
  std::string GetName() const {
    return measure_.GetDescriptor().name();
  }

  /// Record the value for this metric.
  void Record(double value) {
    Record(value, {});
  }

  /// Record the value for this metric.
  ///
  /// \param value The value that we record.
  /// \param tags The tag values that we want to record for this metric record.
  void Record(double value, const std::vector<std::pair<opencensus::tags::TagKey, std::string>>& tags) {
    std::vector<std::pair<opencensus::tags::TagKey, std::string>> combined_tags(tags);
    combined_tags.insert(std::end(combined_tags), std::begin(GlobalTags), std::end(GlobalTags));

    opencensus::stats::Record({{this->measure_, value}}, combined_tags);
  }

 private:
  Metric(const std::string &name,
         const std::string &description,
         const std::string &unit)
    : measure_(opencensus::stats::Measure<double>::Register(name, description, unit)) {};

 private:
  opencensus::stats::Measure<double> measure_;

}; // class Metric

}  // namespace stats

}  // namespace ray

#endif // RAY_STATS_METRIC_H_
