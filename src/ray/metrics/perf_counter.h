#ifndef RAY_METRICS_PERF_COUNTER_H
#define RAY_METRICS_PERF_COUNTER_H

#include <boost/asio.hpp>

#include "ray/metrics/metrics_conf.h"


namespace ray {

#define METRICS_UPDATE_COUNTER(metrics_name, value) \
  metrics::PerfCounter::GetInstance()->UpdateCounter(metrics_name, value)

#define METRICS_UPDATE_COUNTER_WITH_TAGS(metrics_name, value, tags) \
  metrics::PerfCounter::GetInstance()->UpdateCounter(metrics_name, value, tags)

#define METRICS_RESET_GAUGE(metrics_name, value) \
  metrics::PerfCounter::GetInstance()->UpdateGauge(metrics_name, value)

#define METRICS_RESET_GAUGE_WITH_TAGS(metrics_name, value, tags) \
  metrics::PerfCounter::GetInstance()->UpdateGauge(metrics_name, value, tags)

#define METRICS_UPDATE_HISTOGRAM( \
  metrics_name, value, min_value, max_value) \
  metrics::PerfCounter::GetInstance()->UpdateHistogram( \
  metrics_name, value, min_value, max_value)

#define METRICS_UPDATE_HISTOGRAM_WITH_TAGS( \
  metrics_name, value, min_value, max_value, tags) \
  metrics::PerfCounter::GetInstance()->UpdateHistogram( \
  metrics_name, value, min_value, max_value, tags)

namespace metrics {

class PerfCounter final {
 public:
  static PerfCounter *GetInstance();

  // Deleted methods to avoid creating instance with out `GetInstance()`.
  PerfCounter(const PerfCounter &) = delete;

  PerfCounter &operator=(const PerfCounter &) = delete;

  /// Initialize the PerfCounter functions.
  ///
  /// \param conf The configuration of metrics.
  /// \return True for success, and false for failure.
  bool Start(const MetricsConf &conf);

  /// Shutdown the PerfCounter.
  void Shutdown();

  /// Update counter metric.
  ///
  /// \param metrics_name The name of the metric that we want to update.
  /// \param value The value that we want to update to.
  /// \param tags The tags that we want to attach to.
  void UpdateCounter(const std::string &metrics_name,
                     double value,
                     const Tags &tags = Tags{});

  /// Update gauge metric.
  ///
  /// \param metrics_name The name of the metric that we want to update.
  /// \param value The value that we want to update to.
  /// \param tags The tags that we want to attach to.
  void UpdateGauge(const std::string &metrics_name,
                   double value,
                   const Tags &tags = Tags{});

  /// Update histogram metric.
  /// The reasonable range of fluctuation is[min_value, max_value].
  /// Exceeding the range can still be counted, but the accuracy is lower.
  ///
  /// \param metrics_name The name of the metric that we want to update.
  /// \param value The value that we want to update to.
  /// \param min_value The minimum value that we can specified.
  /// \param max_value The maximum value that we can specified.
  /// \param tags The tags that we want to attach to.
  void UpdateHistogram(const std::string &metrics_name,
                       double value,
                       double min_value,
                       double max_value,
                       const Tags &tags = Tags{});

  MetricsRegistryInterface *GetRegistry();

 private:
  PerfCounter() = default;
  ~PerfCounter() = default;

 private:
  class Impl;
  std::unique_ptr<Impl> impl_ptr_;
};

}  // namespace metrics

}  // namespace ray

#endif  // RAY_METRICS_PERF_COUNTER_H
