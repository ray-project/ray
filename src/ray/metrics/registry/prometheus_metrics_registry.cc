#include "prometheus_metrics_registry.h"

#include "ray/util/logging.h"

namespace ray {

namespace metrics {

thread_local std::unordered_map<std::string, std::shared_ptr<MetricFamily>>
    PrometheusMetricsRegistry::metric_map_;

MetricFamily::MetricFamily(MetricType type, const std::string &metric_name,
                           prometheus::Registry *registry, const Tags *tags,
                           std::vector<double> bucket_boundaries)
    : type_(type), bucket_boundaries_(std::move(bucket_boundaries)) {
  switch (type_) {
  case MetricType::kCounter:
    if (tags != nullptr) {
      counter_family_ = &prometheus::BuildCounter()
                             .Name(metric_name)
                             .Labels(tags->GetTags())
                             .Register(*registry);
    } else {
      counter_family_ = &prometheus::BuildCounter().Name(metric_name).Register(*registry);
    }
    break;
  case MetricType::kGauge:
    if (tags != nullptr) {
      gauge_family_ = &prometheus::BuildGauge()
                           .Name(metric_name)
                           .Labels(tags->GetTags())
                           .Register(*registry);
    } else {
      gauge_family_ = &prometheus::BuildGauge().Name(metric_name).Register(*registry);
    }
    break;
  case MetricType::kHistogram:
    if (tags != nullptr) {
      histogram_family_ = &prometheus::BuildHistogram()
                               .Name(metric_name)
                               .Labels(tags->GetTags())
                               .Register(*registry);
    } else {
      histogram_family_ =
          &prometheus::BuildHistogram().Name(metric_name).Register(*registry);
    }
    break;
  default:
    RAY_DCHECK(0);
    return;
  }
}

void MetricFamily::UpdateValue(double value, const Tags *tags) {
  switch (type_) {
  case MetricType::kCounter: {
    prometheus::Counter &counter = GetCounter(tags);
    counter.Increment(value);
  } break;
  case MetricType::kGauge: {
    prometheus::Gauge &gauge = GetGauge(tags);
    gauge.Set(value);
  } break;
  case MetricType::kHistogram: {
    prometheus::Histogram &histogram = GetHistogram(tags);
    histogram.Observe(value);
  } break;
  default:
    RAY_DCHECK(0);
    return;
  }
}

prometheus::Counter &MetricFamily::GetCounter(const Tags *tags) {
  if (tags == nullptr) {
    std::map<std::string, std::string> labels;
    return counter_family_->Add(labels);
  }

  auto it = tag_to_counter_map_.find(tags->GetID());
  if (it != tag_to_counter_map_.end()) {
    return it->second;
  }
  prometheus::Counter &counter = counter_family_->Add(tags->GetTags());
  tag_to_counter_map_.emplace(tags->GetID(), counter);
  return counter;
}

prometheus::Gauge &MetricFamily::GetGauge(const Tags *tags) {
  if (tags == nullptr) {
    std::map<std::string, std::string> labels;
    return gauge_family_->Add(labels);
  }

  auto it = tag_to_gauge_map_.find(tags->GetID());
  if (it != tag_to_gauge_map_.end()) {
    return it->second;
  }
  prometheus::Gauge &gauge = gauge_family_->Add(tags->GetTags());
  tag_to_gauge_map_.emplace(tags->GetID(), gauge);
  return gauge;
}

prometheus::Histogram &MetricFamily::GetHistogram(const Tags *tags) {
  if (tags == nullptr) {
    std::map<std::string, std::string> labels;
    return histogram_family_->Add(labels, bucket_boundaries_);
  }

  auto it = tag_to_histogram_map_.find(tags->GetID());
  if (it != tag_to_histogram_map_.end()) {
    return it->second;
  }
  prometheus::Histogram &histogram =
      histogram_family_->Add(tags->GetTags(), bucket_boundaries_);
  tag_to_histogram_map_.emplace(tags->GetID(), histogram);
  return histogram;
}

PrometheusMetricsRegistry::PrometheusMetricsRegistry(const RegistryOption &options)
    : MetricsRegistryInterface(options) {}

void PrometheusMetricsRegistry::ExportMetrics(const std::string &regex_filter,
                                              AnyPtr *any_ptr) {
  RAY_CHECK(any_ptr != nullptr);

  std::vector<prometheus::MetricFamily> *dest_metrics =
      new std::vector<prometheus::MetricFamily>;
  *any_ptr = dest_metrics;

  auto source_metrics = registry_.Collect();
  if (regex_filter == ".*") {
    dest_metrics->insert(dest_metrics->end(), source_metrics.begin(),
                         source_metrics.end());
    return;
  }

  std::regex filter(regex_filter.c_str());
  for (auto &metric : source_metrics) {
    bool match = std::regex_match(metric.name, filter);
    if (match) {
      dest_metrics->emplace_back(std::move(metric));
    }
  }
}

void PrometheusMetricsRegistry::DoRegisterCounter(const std::string &metric_name,
                                                  const Tags *tags) {
  auto it = metric_map_.find(metric_name);
  if (it != metric_map_.end()) {
    return;
  }

  DoRegister(MetricType::kCounter, metric_name, &default_tags_);
}

void PrometheusMetricsRegistry::DoRegisterGauge(const std::string &metric_name,
                                                const Tags *tags) {
  auto it = metric_map_.find(metric_name);
  if (it != metric_map_.end()) {
    return;
  }

  DoRegister(MetricType::kGauge, metric_name, &default_tags_);
}

void PrometheusMetricsRegistry::DoRegisterHistogram(const std::string &metric_name,
                                                    double min_value, double max_value,
                                                    const Tags *tags) {
  std::vector<double> bucket_boundaries =
      GenBucketBoundaries(min_value, max_value, options_.bucket_count_);

  auto it = metric_map_.find(metric_name);
  if (it != metric_map_.end()) {
    return;
  }

  DoRegister(MetricType::kHistogram, metric_name, &default_tags_,
             std::move(bucket_boundaries));
}

void PrometheusMetricsRegistry::DoUpdateValue(const std::string &metric_name,
                                              double value, const Tags *tags) {
  std::shared_ptr<MetricFamily> metric;

  auto it = metric_map_.find(metric_name);
  if (it != metric_map_.end()) {
    metric = it->second;
  }

  if (metric == nullptr) {
    metric = DoRegister(MetricType::kCounter, metric_name, &default_tags_);
  }

  metric->UpdateValue(value, tags);
}

std::shared_ptr<MetricFamily> PrometheusMetricsRegistry::DoRegister(
    MetricType type, const std::string &metric_name, const Tags *tags,
    std::vector<double> bucket_boundaries) {
  auto it = metric_map_.find(metric_name);
  if (it != metric_map_.end()) {
    return it->second;
  }
  auto metric = std::make_shared<MetricFamily>(type, metric_name, &registry_, tags,
                                               std::move(bucket_boundaries));
  metric_map_.emplace(metric_name, metric);
  return metric;
}

}  // namespace metrics

}  // namespace ray
