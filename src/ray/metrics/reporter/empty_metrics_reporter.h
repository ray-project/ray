#ifndef RAY_METRICS_REPORTER_EMPTY_METRICS_REPORTER_H
#define RAY_METRICS_REPORTER_EMPTY_METRICS_REPORTER_H

#include "ray/metrics/reporter/metrics_reporter_interface.h"

namespace ray {

namespace metrics {

class EmptyMetricsReporter : public MetricsReporterInterface {
 public:
  explicit EmptyMetricsReporter(const ReporterOption &options)
      : MetricsReporterInterface(options) {}

  virtual ~EmptyMetricsReporter() = default;

  bool Start() override { return true; }

  bool Stop() override { return true; }

  void RegisterRegistry(MetricsRegistryInterface *registry) override {}
};

}  // namespace metrics

}  // namespace ray

#endif  // RAY_METRICS_REPORTER_EMPTY_METRICS_REPORTER_H
