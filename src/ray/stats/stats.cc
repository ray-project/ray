#include "ray/stats/stats.h"

namespace ray {

namespace stats {
std::shared_ptr<IOServicePool> metrics_io_service_pool;
std::shared_ptr<MetricExporterClient> exporter;
absl::Mutex stats_mutex;
}  // namespace stats

}  // namespace ray
