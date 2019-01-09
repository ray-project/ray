#ifndef RAY_METRICS_REGISTRY_PROMETHEUS_METRICS_REGISTRY_H
#define RAY_METRICS_REGISTRY_PROMETHEUS_METRICS_REGISTRY_H

#include <unordered_map>

#include "prometheus/registry.h"
#include "ray/metrics/registry/metrics_registry_interface.h"

namespace ray {

namespace metrics {

class MetricFamily {
 public:
  MetricFamily(MetricType type, const std::string &metric_name,
               prometheus::Registry *registry, const Tags *tags = nullptr,
               std::vector<double> bucket_boundaries = {});

  ~MetricFamily() = default;

  /// Update value with tags
  void UpdateValue(double value, const Tags *tags = nullptr);

 private:
  /// Get counter by tags
  prometheus::Counter &GetCounter(const Tags *tags);
  /// Get gauge by tags
  prometheus::Gauge &GetGauge(const Tags *tags);
  /// Get histogram by tags
  prometheus::Histogram &GetHistogram(const Tags *tags);

  MetricType type_;
  /// Container of all counters
  prometheus::Family<prometheus::Counter> *counter_family_{nullptr};
  /// The counter object corresponding to each tags
  std::unordered_map<size_t, prometheus::Counter &> tag_to_counter_map_;
  /// Container of all gauges
  prometheus::Family<prometheus::Gauge> *gauge_family_{nullptr};
  /// The gauge object corresponding to each tags
  std::unordered_map<size_t, prometheus::Gauge &> tag_to_gauge_map_;
  /// Container of all histogram
  prometheus::Family<prometheus::Histogram> *histogram_family_{nullptr};
  /// The histogram object corresponding to each tags
  std::unordered_map<size_t, prometheus::Histogram &> tag_to_histogram_map_;
  /// Boundary of histogram bucket
  std::vector<double> bucket_boundaries_;
};

class PrometheusMetricsRegistry : public MetricsRegistryInterface {
 public:
  explicit PrometheusMetricsRegistry(const RegistryOption &options);

  virtual ~PrometheusMetricsRegistry() = default;

  void ExportMetrics(const std::string &regex_filter, AnyPtr *any_ptr) override;

 protected:
  void DoRegisterCounter(const std::string &metric_name, const Tags *tags) override;

  void DoRegisterGauge(const std::string &metric_name, const Tags *tags) override;

  void DoRegisterHistogram(const std::string &metric_name, double min_value,
                           double max_value, const Tags *tags) override;

  void DoUpdateValue(const std::string &metric_name, double value,
                     const Tags *tags) override;

 private:
  std::shared_ptr<MetricFamily> DoRegister(MetricType type,
                                           const std::string &metric_name,
                                           const Tags *tags,
                                           std::vector<double> bucket_boundaries = {});

 private:
  /// Prometheus registry
  prometheus::Registry registry_;
  /// Thread local metrics
  static thread_local std::unordered_map<std::string, std::shared_ptr<MetricFamily>>
      metric_map_;
};

}  // namespace metrics

}  // namespace ray

#endif  // RAY_METRICS_REGISTRY_PROMETHEUS_METRICS_REGISTRY_H
