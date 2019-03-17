#ifndef _RAY_STATS_H_
#define _RAY_STATS_H_

#include <string>

#include "opencensus/exporters/stats/prometheus/prometheus_exporter.h"
#include "prometheus/exposer.h"
#include "opencensus/tags/tag_key.h"
#include "opencensus/stats/stats.h"

#include "ray/stats/metric.h"

namespace ray {

namespace stats {

/// Include metric_defs.h to define measure items.
#include "metric_defs.h"

  /// Initialize perf counter.
  static void Init(const std::string &address) {
    // Enable the Prometheus exporter.
    // Note that the reason for we using local static variables
    // here is to make sure they are single-instances.
    static auto exporter = std::make_shared<opencensus::exporters::stats::PrometheusExporter>();
    static prometheus::Exposer exposer(address);
    exposer.RegisterCollectable(exporter);
  }

} // namespace stats


} // namespace ray

#endif // _RAY_STATS_H_
