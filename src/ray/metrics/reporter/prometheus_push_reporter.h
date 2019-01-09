#ifndef RAY_METRICS_REPORTER_PROMETHEUS_PUSH_REPORTER_H
#define RAY_METRICS_REPORTER_PROMETHEUS_PUSH_REPORTER_H

#include <atomic>
#include <boost/asio.hpp>
#include <memory>
#include <mutex>
#include <thread>

#include "prometheus/collectable.h"
#include "prometheus/gateway.h"
#include "ray/metrics/registry/metrics_registry_interface.h"
#include "ray/metrics/reporter/metrics_reporter_interface.h"

namespace ray {

namespace metrics {

class RegistryExportHandler : public prometheus::Collectable {
 public:
  RegistryExportHandler(const std::string &regex_filter,
                        MetricsRegistryInterface *registry);

  std::vector<prometheus::MetricFamily> Collect() override;

 private:
  const std::string &regex_filter_;
  MetricsRegistryInterface *registry_;
};

class PrometheusPushReporter : public MetricsReporterInterface {
 public:
  explicit PrometheusPushReporter(const ReporterOption &options);

  virtual ~PrometheusPushReporter();

  void RegisterRegistry(MetricsRegistryInterface *registry) override;

  bool Start() override;

  bool Stop() override;

 private:
  void ThreadReportAction();

  std::mutex mutex_;
  /// Registry handler map
  std::unordered_map<MetricsRegistryInterface *, std::shared_ptr<RegistryExportHandler>>
      exporter_handler_;
  /// Prometheus gateway
  std::unique_ptr<prometheus::Gateway> gate_way_;
  /// Whether the reporter stopped
  std::atomic<bool> is_stopped{false};
  /// A thread that reporting (synchronous mode)
  /// every ReporterOption.report_interval_ seconds.
  std::unique_ptr<std::thread> report_thread_;
};

}  // namespace metrics

}  // namespace ray

#endif  // RAY_METRICS_REPORTER_PROMETHEUS_PUSH_REPORTER_H
