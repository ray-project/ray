#ifndef RAY_METRICS_REGISTRY_EMPTY_METRICS_REGISTRY_H
#define RAY_METRICS_REGISTRY_EMPTY_METRICS_REGISTRY_H

#include "ray/metrics/registry/metrics_registry_interface.h"

namespace ray {

namespace metrics {

class EmptyMetricsRegistry : public MetricsRegistryInterface {
 public:
  explicit EmptyMetricsRegistry(const RegistryOption &options)
      : MetricsRegistryInterface(options) {}

  virtual ~EmptyMetricsRegistry() {}

  void ExportMetrics(const std::string &regex_filter, AnyPtr *any_ptr) override {}

 protected:
  void DoRegisterCounter(const std::string &metric_name, const Tags *tags) override {}

  void DoRegisterGauge(const std::string &metric_name, const Tags *tags) override {}

  void DoRegisterHistogram(const std::string &metric_name, double min_value,
                           double max_value, const Tags *tags) override {}

  void DoUpdateValue(const std::string &metric_name, double value,
                     const Tags *tags) override {}
};

}  // namespace metrics

}  // namespace ray

#endif  // RAY_METRICS_REGISTRY_EMPTY_METRICS_REGISTRY_H
