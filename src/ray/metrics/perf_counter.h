#ifndef _RAY_PERF_COUNTER_H_
#define _RAY_PERF_COUNTER_H_

#include <string>

#include "opencensus/exporters/stats/prometheus/prometheus_exporter.h"
#include "prometheus/exposer.h"
#include "opencensus/tags/tag_key.h"
#include "opencensus/stats/stats.h"

namespace ray {

namespace perf_counter {

/// Include tag_def.h to define tag items
#define RAY_TAG(name)                                                       \
  static opencensus::tags::TagKey name##Key() {                             \
    static const auto key = opencensus::tags::TagKey::Register(#name);      \
    return key;                                                             \
  }

#include "tag_def.h"
#undef RAY_TAG


/// A thin wrapper that wraps the `opencensus::tag::measure` for using it simply.
class Metric final {

 public:
  Metric(const std::string &name,
         const std::string &description,
         const std::string &unit) {
    this->measure_ = std::make_shared<opencensus::stats::Measure<double>>(
        opencensus::stats::Measure<double>::Register(
            name, description, unit));
  }

  ~Metric() = default;

  std::string GetName() const {
    return measure_->GetDescriptor().name();
  }

  void Record(double value) {
    Record(value, {});
  }

  void Record(double value, const std::vector<std::pair<opencensus::tags::TagKey::TagKey, std::string>>& tags) {
    // global tags should be registered here.
    static std::vector<std::pair<opencensus::tags::TagKey, std::string>> global_tags = {
        {ray::perf_counter::JobNameKey(), "raylet"}
    };

    std::vector<std::pair<opencensus::tags::TagKey, std::string>> combined_tags(tags);
    combined_tags.insert(std::end(combined_tags), std::begin(global_tags), std::end(global_tags));
    opencensus::stats::Record({{*this->measure_, value}}, combined_tags);
  }

 private:
  std::shared_ptr<opencensus::stats::Measure<double>> measure_ = nullptr;

}; // class Metric

/// Include metric_def.h to define tag items
#define RAY_METRIC(name, description, unit)                                 \
  static Metric name##_metric_ = Metric(std::string("raylet/") + #name,     \
                                        description, unit);                 \
  static Metric &name() {                                                   \
    return name##_metric_;                                                  \
  }

#include "metrics_def.h"
#undef RAY_METRIC


/// The helper function for registering a view.
  static void RegisterAsView(opencensus::stats::ViewDescriptor view_descriptor) {
    opencensus::stats::View view(view_descriptor);
    view_descriptor.RegisterForExport();
  }


/// The helper function for registering all views.
  static void RegisterAllViews() {
    {
      const opencensus::stats::ViewDescriptor view_descriptor =
          opencensus::stats::ViewDescriptor().set_name("raylet/latency")
              .set_description("The latency of redis.")
              .set_measure(RedisLatency().GetName())
              .set_aggregation(opencensus::stats::Aggregation::Distribution(
                  opencensus::stats::BucketBoundaries::Explicit({0, 25, 50, 75, 1000})))
              .add_column(JobNameKey());

      RegisterAsView(view_descriptor);
    }

    {
      const opencensus::stats::ViewDescriptor view_descriptor =
          opencensus::stats::ViewDescriptor().set_name("raylet/task_elapse")
              .set_description("task elapse")
              .set_measure(TaskElapse().GetName())
              .set_aggregation(opencensus::stats::Aggregation::Distribution(
                  opencensus::stats::BucketBoundaries::Explicit({100, 250, 500, 750, 1000})))
              .add_column(JobNameKey())
              .add_column(NodeAddressKey());

      RegisterAsView(view_descriptor);
    }

    {
      const opencensus::stats::ViewDescriptor view_descriptor =
          opencensus::stats::ViewDescriptor().set_name("raylet/task_count")
              .set_description("task count")
              .set_measure(TaskCount().GetName())
              .set_aggregation(opencensus::stats::Aggregation::LastValue())
              .add_column(JobNameKey())
              .add_column(NodeAddressKey());

      RegisterAsView(view_descriptor);
    }

  }


/// Initialize perf counter.
  static void Init(const std::string &address) {
    // Enable the Prometheus exporter.
    // Note that the reason for we using local static variables
    // here is to make sure they are single instances.
    static auto exporter = std::make_shared<opencensus::exporters::stats::PrometheusExporter>();
    static prometheus::Exposer exposer(address);
    exposer.RegisterCollectable(exporter);

    ray::perf_counter::RegisterAllViews();
  }

} // namespace perf_counter


} // namespace ray

#endif // _RAY_PERF_COUNTER_H_
