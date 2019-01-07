#ifndef RAY_METRICS_REGISTRY_METRICS_REGISTRY_INTERFACE_H
#define RAY_METRICS_REGISTRY_METRICS_REGISTRY_INTERFACE_H

#include <map>
#include <regex>
#include <string>
#include <unordered_set>

#include "ray/metrics/tag/tags.h"
#include "ray/metrics/registry/any_ptr.h"

namespace ray {

namespace metrics {

enum class MetricType : int8_t {
  kCounter,
  kGauge,
  kHistogram,
};

class RegistryOption {
 public:
  RegistryOption() = default;

  // Global tags as the default tags for all metrics of the registry.
  std::map<std::string, std::string> default_tag_map_{};

  // Percentiles for each histogram metric.
  std::unordered_set<double> default_percentiles_{0.01, 1, 60, 90, 99, 99.99};
  // Bucket count for each histogram metric.
  // The more the number of buckets, the more accurate, but the greater the overhead.
  size_t bucket_count_{100};
};

class MetricsRegistryInterface {
 public:
  virtual ~MetricsRegistryInterface() = default;

  // noncopyable
  MetricsRegistryInterface(const MetricsRegistryInterface &) = delete;
  MetricsRegistryInterface &operator=(const MetricsRegistryInterface &) = delete;

  /// Register a counter metric.
  ///
  /// \param metrics_name The name of the metric that we want to register.
  /// \param tags The tags that we want to attach to this metric.
  void RegisterCounter(const std::string &metric_name, const Tags *tags = nullptr) {
    DoRegisterCounter(metric_name, tags);
  }

  /// Register a gauge metric.
  ///
  /// \param metrics_name The name of the metric that we want to register.
  /// \param tags The tags that we want to attach to this metric.
  void RegisterGauge(const std::string &metric_name, const Tags *tags = nullptr) {
    DoRegisterGauge(metric_name, tags);
  }

  /// Register a histogram metric.
  /// The reasonable range of fluctuation is[min_value, max_value].
  /// Exceeding the range can still be counted, but the accuracy is lower.
  ///
  /// \param metrics_name The name of the metric that we want to register.
  /// \param min_value The minimum value that the metric could be.
  /// \param max_value The maximum value that the metric could be.
  /// \param tags The tags that we want to attach to this metric.
  void RegisterHistogram(const std::string &metric_name, double min_value,
                         double max_value, const Tags *tags = nullptr) {
    DoRegisterHistogram(metric_name, min_value, max_value, tags);
  }

  /// Update the value of metric.
  /// If metric registered as counter: increase value.
  /// If metric registered as gauge: reset to the input value.
  /// If metric registered as historgram: add a new sample which value is the input value.
  ///
  /// \param metrics_name The name of the metric that we want to update.
  /// \param value The value that we want to update.
  /// \param tags The tags that attached to this value.
  void UpdateValue(const std::string &metric_name, double value,
                   const Tags *tags = nullptr) {
    DoUpdateValue(metric_name, value, tags);
  }

  /// Export all registered metrics which match the filter.
  ///
  /// \param regex_filter Regex expression to filter the metrics.
  /// As we all known regex ".*" match all values.
  /// \param any_ptr Result after filter.
  virtual void ExportMetrics(const std::string &regex_filter, AnyPtr *any_ptr) = 0;

  /// Get global tags.
  virtual const Tags &GetDefaultTags() { return default_tags_; }

 protected:
  explicit MetricsRegistryInterface(const RegistryOption &options)
      : default_tags_(options.default_tag_map_), options_(options) {}

  virtual void DoRegisterCounter(const std::string &metric_name, const Tags *tags) = 0;

  virtual void DoRegisterGauge(const std::string &metric_name, const Tags *tags) = 0;

  virtual void DoRegisterHistogram(const std::string &metric_name, double min_value,
                                   double max_value, const Tags *tags) = 0;

  virtual void DoUpdateValue(const std::string &metric_name, double value,
                             const Tags *tags) = 0;

  /// Divided range[min_value, max_value] into multiple buckets for histogram statistics.
  std::vector<double> GenBucketBoundaries(double min_value, double max_value,
                                          size_t bucket_count) const;

  Tags default_tags_;
  RegistryOption options_;
};

}  // namespace metrics

}  // namespace ray

#endif  // RAY_METRICS_REGISTRY_METRICS_REGISTRY_INTERFACE_H
