#ifndef _RAY_STATS_H_
#define _RAY_STATS_H_

#include <string>

#include "opencensus/exporters/stats/prometheus/prometheus_exporter.h"
#include "prometheus/exposer.h"
#include "opencensus/tags/tag_key.h"
#include "opencensus/stats/stats.h"

namespace ray {

namespace stats {

/// Include tag_defs.h to define tag items
#include "tag_defs.h"

/// The helper function for registering a view.
static void RegisterAsView(opencensus::stats::ViewDescriptor view_descriptor) {
  opencensus::stats::View view(view_descriptor);
  view_descriptor.RegisterForExport();
}

/// A thin wrapper that wraps the `opencensus::tag::measure` for using it simply.
class Metric final {

 private:
  Metric(const std::string &name,
         const std::string &description,
         const std::string &unit)
    : measure_(opencensus::stats::Measure<double>::Register(name, description, unit)) {}

 public:
  ~Metric() = default;

  Metric& operator()() {
    return *this;
  }

  /// Last value
  static Metric MakeGauge(const std::string &name,
                          const std::string &description,
                          const std::string &unit,
                          const std::vector<opencensus::tags::TagKey>& tag_keys = {}) {
    auto metric = Metric(name, description, unit);

    opencensus::stats::ViewDescriptor view_descriptor =
          opencensus::stats::ViewDescriptor().set_name(std::string("raylet/") + name)
              .set_description(description)
              .set_measure(metric.GetName())
              .set_aggregation(opencensus::stats::Aggregation::LastValue());
              // TODO(qwang): .add_column(AllGlobalTagKeys)

      for (const auto &tag_key : tag_keys) {
        view_descriptor = view_descriptor.add_column(tag_key);
      }

       RegisterAsView(view_descriptor);

    return metric;
  }

  /// Histgrom
  static Metric MakeHistogram(const std::string &name,
                          const std::string &description,
                          const std::string &unit,
                          const std::vector<double> boundaries,
                          const std::vector<opencensus::tags::TagKey>& tag_keys = {}) {
    auto metric = Metric(name, description, unit); 
    opencensus::stats::ViewDescriptor view_descriptor =
          opencensus::stats::ViewDescriptor().set_name(name)
              .set_description(description)
              .set_measure(metric.GetName())
              .set_aggregation(opencensus::stats::Aggregation::Distribution(
                opencensus::stats::BucketBoundaries::Explicit(boundaries)));
              // TODO(qwang): .add_column(AllGlobalTagKeys)

      for (const auto &tag_key : tag_keys) {
        view_descriptor = view_descriptor.add_column(tag_key);
      }

    RegisterAsView(view_descriptor);
    
    return metric;
  }

  std::string GetName() const {
    return measure_.GetDescriptor().name();
  }

  void Record(double value) {
    Record(value, {});
  }

  void Record(double value, const std::vector<std::pair<opencensus::tags::TagKey::TagKey, std::string>>& tags) {
    // global tags should be registered here.
    static std::vector<std::pair<opencensus::tags::TagKey, std::string>> global_tags = {
        {ray::stats::JobNameKey, "raylet"}
    };

    std::vector<std::pair<opencensus::tags::TagKey, std::string>> combined_tags(tags);
    combined_tags.insert(std::end(combined_tags), std::begin(global_tags), std::end(global_tags));
    opencensus::stats::Record({{this->measure_, value}}, combined_tags);
  }

 private:
  opencensus::stats::Measure<double> measure_;

}; // class Metric

/// Include metric_defs.h to define tag items
#include "metric_defs.h"

  /// Initialize perf counter.
  static void Init(const std::string &address) {
    // Enable the Prometheus exporter.
    // Note that the reason for we using local static variables
    // here is to make sure they are single instances.
    static auto exporter = std::make_shared<opencensus::exporters::stats::PrometheusExporter>();
    static prometheus::Exposer exposer(address);
    exposer.RegisterCollectable(exporter);
  }

} // namespace stats


} // namespace ray

#endif // _RAY_STATS_H_
