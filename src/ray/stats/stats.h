#ifndef RAY_STATS_STATS_H_
#define RAY_STATS_STATS_H_

#include <string>

#include "opencensus/exporters/stats/stdout/stdout_exporter.h"
#include "opencensus/exporters/stats/prometheus/prometheus_exporter.h"
#include "prometheus/exposer.h"
#include "opencensus/tags/tag_key.h"
#include "opencensus/stats/stats.h"

#include "ray/stats/metric.h"

namespace ray {

namespace stats {

/// Include metric_defs.h to define measure items.
#include "metric_defs.h"

  /// Initialize stats.
  static void Init(const std::string &address) {
    // Enable the Prometheus exporter.
    // Note that the reason for we using local static variables
    // here is to make sure they are single-instances.
    static auto exporter = std::make_shared<opencensus::exporters::stats::PrometheusExporter>();
    static prometheus::Exposer exposer(address);
    exposer.RegisterCollectable(exporter);

    // Also enable stdout exporter by default.
    opencensus::exporters::stats::StdoutExporter::Register();
  }

} // namespace stats


} // namespace ray

#endif // RAY_STATS_STATS_H_
