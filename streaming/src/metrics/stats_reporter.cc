#include "metrics/stats_reporter.h"
#include "util/streaming_logging.h"

namespace ray {
namespace streaming {

std::shared_ptr<ray::stats::Metric> StatsReporter::GetMetricByName(
    const std::string &metric_name) {
  std::unique_lock<std::mutex> lock(metric_mutex_);
  auto metric = metric_map_.find(metric_name);
  if (metric != metric_map_.end()) {
    return metric->second;
  }
  return nullptr;
}

void StatsReporter::MetricRegister(const std::string &metric_name,
                                   std::shared_ptr<ray::stats::Metric> metric) {
  std::unique_lock<std::mutex> lock(metric_mutex_);
  metric_map_[metric_name] = metric;
}

void StatsReporter::UnregisterAllMetrics() {
  std::unique_lock<std::mutex> lock(metric_mutex_);
  metric_map_.clear();
}

bool StatsReporter::Start(const StreamingMetricsConfig &conf) {
  global_tags_ = conf.GetMetricsGlobalTags();
  service_name_ = conf.GetMetricsServiceName();
  STREAMING_LOG(INFO) << "Start stats reporter, service name " << service_name_
                      << ", global tags size : " << global_tags_.size()
                      << ", stats disabled : "
                      << stats::StatsConfig::instance().IsStatsDisabled();
  for (auto &tag : global_tags_) {
    global_tag_key_list_.push_back(stats::TagKeyType::Register(tag.first));
  }
  return true;
}

bool StatsReporter::Start(const std::string &json_string) { return true; }

StatsReporter::~StatsReporter() {
  STREAMING_LOG(WARNING) << "stats client shutdown";
  Shutdown();
};

void StatsReporter::Shutdown() { UnregisterAllMetrics(); }

void StatsReporter::UpdateCounter(const std::string &domain,
                                  const std::string &group_name,
                                  const std::string &short_name, double value) {
  const std::string merged_metric_name =
      METRIC_GROUP_JOIN(domain, group_name, short_name);
}

void StatsReporter::UpdateCounter(
    const std::string &metric_name,
    const std::unordered_map<std::string, std::string> &tags, double value) {
  STREAMING_LOG(DEBUG) << "Report counter metric " << metric_name << " , value " << value;
}

void StatsReporter::UpdateGauge(const std::string &domain, const std::string &group_name,
                                const std::string &short_name, double value,
                                bool is_reset) {
  const std::string merged_metric_name =
      service_name_ + "." + METRIC_GROUP_JOIN(domain, group_name, short_name);
  STREAMING_LOG(DEBUG) << "Report gauge metric " << merged_metric_name << " , value "
                       << value;
  auto metric = GetMetricByName(merged_metric_name);
  if (nullptr == metric) {
    metric = std::shared_ptr<ray::stats::Metric>(
        new ray::stats::Gauge(merged_metric_name, "", "", global_tag_key_list_));
    MetricRegister(merged_metric_name, metric);
  }
  metric->Record(value, global_tags_);
}

void StatsReporter::UpdateGauge(const std::string &metric_name,
                                const std::unordered_map<std::string, std::string> &tags,
                                double value, bool is_reset) {
  const std::string merged_metric_name = service_name_ + "." + metric_name;
  STREAMING_LOG(DEBUG) << "Report gauge metric " << merged_metric_name << " , value "
                       << value;
  // Get metric from registered map, create a new one item if no such metric can be found
  // in map.
  auto metric = GetMetricByName(metric_name);
  if (nullptr == metric) {
    // Register tag key for all tags.
    std::vector<stats::TagKeyType> tag_key_list(global_tag_key_list_.begin(),
                                                global_tag_key_list_.end());
    for (auto &tag : tags) {
      tag_key_list.push_back(stats::TagKeyType::Register(tag.first));
    }
    metric = std::shared_ptr<ray::stats::Metric>(
        new ray::stats::Gauge(merged_metric_name, "", "", tag_key_list));
    MetricRegister(merged_metric_name, metric);
  }
  auto merged_tags = MergeGlobalTags(tags);
  metric->Record(value, merged_tags);
}

void StatsReporter::UpdateHistogram(const std::string &domain,
                                    const std::string &group_name,
                                    const std::string &short_name, double value,
                                    double min_value, double max_value) {}

void StatsReporter::UpdateHistogram(
    const std::string &metric_name,
    const std::unordered_map<std::string, std::string> &tags, double value,
    double min_value, double max_value) {}

void StatsReporter::UpdateQPS(const std::string &metric_name,
                              const std::unordered_map<std::string, std::string> &tags,
                              double value) {}
}  // namespace streaming
}  // namespace ray
