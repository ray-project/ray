#include "prometheus_push_reporter.h"

#include "ray/util/logging.h"

namespace ray {

namespace metrics {

RegistryExportHandler::RegistryExportHandler(const std::string &regex_filter,
                                             MetricsRegistryInterface *registry)
    : regex_filter_(regex_filter), registry_(registry) {}

std::vector<prometheus::MetricFamily> RegistryExportHandler::Collect() {
  AnyPtr any_ptr;
  registry_->ExportMetrics(regex_filter_, &any_ptr);
  auto *src_metrics = any_ptr.CastTo<std::vector<prometheus::MetricFamily>>();

  std::vector<prometheus::MetricFamily> dest_metrics;
  if (src_metrics != nullptr) {
    dest_metrics.swap(*src_metrics);
  } else {
    RAY_LOG(DEBUG) << "Registry not match PrometheusPushReporter.";
  }
  return dest_metrics;
}

PrometheusPushReporter::PrometheusPushReporter(const ReporterOption &options)
    : MetricsReporterInterface(options) {
  gate_way_.reset(new prometheus::Gateway(options_.service_addr_, options_.job_name_, {},
                                          options_.user_name_, options_.password_));
}

PrometheusPushReporter::~PrometheusPushReporter() {}

void PrometheusPushReporter::RegisterRegistry(MetricsRegistryInterface *registry) {
  if (registry != nullptr) {
    std::shared_ptr<RegistryExportHandler> export_handler =
        std::make_shared<RegistryExportHandler>(options_.regex_filter_, registry);
    std::map<std::string, std::string> tag_map = registry->GetDefaultTags().GetTags();
    tag_map.emplace("step", std::to_string(options_.report_interval_.count()));
    gate_way_->RegisterCollectable(export_handler, &tag_map);
    {
      std::lock_guard<std::mutex> lock(mutex_);
      exporter_handler_.emplace(registry, export_handler);
    }
  }
}

bool PrometheusPushReporter::Start() {
  if (options_.service_addr_.empty() || options_.job_name_.empty()) {
    RAY_LOG(WARNING) << "Prometheus Init failed, invalid service addr or job name, "
                     << " address=" << options_.service_addr_
                     << " job_name=" << options_.job_name_;
    return false;
  }

  report_thread_.reset(
      new std::thread(std::bind(&PrometheusPushReporter::ThreadReportAction, this)));
  return true;
}

void PrometheusPushReporter::ThreadReportAction() {
  while (!is_stopped.load(std::memory_order_acquire)) {
    std::this_thread::sleep_for(options_.report_interval_);
    // TODO(micafan) AsyncPush
    int ret_code = gate_way_->Push();
    int64_t left_retry_times = options_.max_retry_times_;
    // Retry
    if (ret_code == 200) {
      RAY_LOG(DEBUG) << "Prometheus report ret_code=" << ret_code
                     << " address=" << options_.service_addr_
                     << " job_name=" << options_.job_name_;
    } else {
      RAY_LOG(WARNING) << "Prometheus report failed, ret_code=" << ret_code
                       << " address=" << options_.service_addr_
                       << " job_name=" << options_.job_name_
                       << " left_retry_times=" << left_retry_times;
    }
    while (ret_code != 200 && (left_retry_times-- > 0)) {
      std::chrono::milliseconds wait_time(100);
      std::this_thread::sleep_for(wait_time);
      ret_code = gate_way_->Push();
    }
  }
}

bool PrometheusPushReporter::Stop() {
  bool expected = false;
  if (!std::atomic_compare_exchange_strong(&is_stopped, &expected, true)) {
    return false;
  }

  if (report_thread_ != nullptr) {
    report_thread_->join();
  }

  return true;
}

}  // namespace metrics

}  // namespace ray
