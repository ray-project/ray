#include <sstream>

#include "metrics/stats_reporter.h"
#include "metrics/streaming_perf_metric.h"
#include "util/streaming_logging.h"

namespace ray {
namespace streaming {

bool StreamingReporter::Start(const StreamingMetricsConfig &conf) {
  if (impl_) {
    STREAMING_LOG(WARNING) << "Streaming perf is active";
  } else {
    impl_.reset(new StatsReporter());
    return impl_->Start(conf);
  }
  return false;
}

void StreamingReporter::Shutdown() {
  if (impl_) {
    impl_->Shutdown();
    impl_.reset();
  } else {
    STREAMING_LOG(WARNING) << "No active perf instance will be shutdown";
  }
}
void StreamingReporter::UpdateCounter(const std::string &domain,
                                      const std::string &group_name,
                                      const std::string &short_name, double value) {
  if (impl_) {
    impl_->UpdateCounter(domain, group_name, short_name, value);
  } else {
    STREAMING_LOG(WARNING) << "No active perf instance";
  }
}

void StreamingReporter::UpdateGauge(const std::string &domain,
                                    const std::string &group_name,
                                    const std::string &short_name, double value,
                                    bool is_reset) {
  if (impl_) {
    impl_->UpdateGauge(domain, group_name, short_name, value, is_reset);
  } else {
    STREAMING_LOG(WARNING) << "No active perf instance";
  }
}

void StreamingReporter::UpdateHistogram(const std::string &domain,
                                        const std::string &group_name,
                                        const std::string &short_name, double value,
                                        double min_value, double max_value) {
  if (impl_) {
    impl_->UpdateHistogram(domain, group_name, short_name, value, min_value, max_value);
  } else {
    STREAMING_LOG(WARNING) << "No active perf instance";
  }
}
void StreamingReporter::UpdateQPS(
    const std::string &metric_name,
    const std::unordered_map<std::string, std::string> &tags, double value) {
  if (impl_) {
    impl_->UpdateQPS(metric_name, tags, value);
  } else {
    STREAMING_LOG(WARNING) << "No active perf instance";
  }
}

StreamingReporter::~StreamingReporter() {
  if (impl_) {
    STREAMING_LOG(INFO) << "Destory streamimg perf => " << impl_.get();
    Shutdown();
  }
}

void StreamingReporter::UpdateCounter(
    const std::string &metric_name,
    const std::unordered_map<std::string, std::string> &tags, double value) {
  if (impl_) {
    impl_->UpdateCounter(metric_name, tags, value);
  }
}
void StreamingReporter::UpdateGauge(
    const std::string &metric_name,
    const std::unordered_map<std::string, std::string> &tags, double value,
    bool is_rest) {
  if (impl_) {
    impl_->UpdateGauge(metric_name, tags, value, is_rest);
  }
}
void StreamingReporter::UpdateHistogram(
    const std::string &metric_name,
    const std::unordered_map<std::string, std::string> &tags, double value,
    double min_value, double max_value) {}

}  // namespace streaming
}  // namespace ray
