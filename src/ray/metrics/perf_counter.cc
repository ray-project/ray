#include "perf_counter.h"

#include <memory>
#include <mutex>

#include "ray/metrics/metrics_conf.h"
#include "ray/metrics/registry/metrics_registry_interface.h"
#include "ray/metrics/reporter/metrics_reporter_interface.h"

namespace ray {

namespace metrics {

class PerfCounter::Impl {
 public:
  Impl() = default;
  virtual ~Impl() = default;

  // noncopyable
  Impl(const Impl &) = delete;
  Impl &operator=(const Impl &) = delete;

  bool Start(const MetricsConf &conf);

  void Shutdown();

  // If not found, we should insert one.
  void UpdateCounter(const std::string &metrics_name,
                     double value,
                     const Tags *tags);

  void UpdateGauge(const std::string &metrics_name,
                   double value,
                   const Tags *tags);

  void UpdateHistogram(const std::string &metrics_name,
                       double value,
                       double min_value,
                       double max_value,
                       const Tags *tags);

  MetricsRegistryInterface *GetRegistry();

 protected:
  void Clear();

 protected:
  MetricsRegistryInterface *registry_{nullptr};
  MetricsReporterInterface *reporter_{nullptr};
};

} // namespace metrics

} // namespace ray
