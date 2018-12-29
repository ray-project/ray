#ifndef RAY_METRICS_REPORTER_METRICS_REPORTER_INTERFACE_H
#define RAY_METRICS_REPORTER_METRICS_REPORTER_INTERFACE_H

#include <chrono>
#include <regex>
#include <string>

#include "ray/metrics/registry/metrics_registry_interface.h"

namespace ray {

namespace metrics {

class ReporterOption {
 public:
  ReporterOption() = default;

  // User name.
  std::string user_name_;
  // Password.
  std::string password_;
  // Job name.
  std::string job_name_;

  // Required field.
  // Address of service. e.g.: Address of gateway.
  std::string service_addr_;

  // Report interval.
  std::chrono::seconds report_interval_{10};

  // Regex filter. Used to filter metrics of registry.
  std::string regex_filter_{".*"};

  // Max retry times when report failed.
  int64_t max_retry_times_{2};
};

class MetricsReporterInterface {
 public:
  virtual ~MetricsReporterInterface() = default;

  /// Register registry to Reporter.
  virtual void RegisterRegistry(MetricsRegistryInterface *registry) = 0;

  /// Start reporter.
  virtual bool Start() = 0;

  /// Stop reporter.
  virtual bool Stop() = 0;

 protected:
  explicit MetricsReporterInterface(const ReporterOption &options)
      : options_(options) {}

  ReporterOption options_;
};

}  // namespace metrics

}  // namespace ray

#endif  // RAY_METRICS_REPORTER_METRICS_REPORTER_INTERFACE_H
