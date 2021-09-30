#include "runtime_context.h"

#include "ray/common/id.h"
#include "ray/util/util.h"
#include "src/ray/protobuf/common.pb.h"
#include "util/streaming_logging.h"
#include "util/streaming_util.h"

namespace ray {
namespace streaming {

void RuntimeContext::SetConfig(const StreamingConfig &streaming_config) {
  STREAMING_CHECK(runtime_status_ == RuntimeStatus::Init)
      << "set config must be at beginning";
  config_ = streaming_config;
}

void RuntimeContext::SetConfig(const uint8_t *data, uint32_t size) {
  STREAMING_CHECK(runtime_status_ == RuntimeStatus::Init)
      << "set config must be at beginning";
  if (!data) {
    STREAMING_LOG(WARNING) << "buffer pointer is null, but len is => " << size;
    return;
  }
  config_.FromProto(data, size);
}

RuntimeContext::~RuntimeContext() {}

RuntimeContext::RuntimeContext()
    : enable_timer_service_(false), runtime_status_(RuntimeStatus::Init) {}

void RuntimeContext::InitMetricsReporter() {
  STREAMING_LOG(INFO) << "init metrics";
  if (!config_.GetMetricsEnable()) {
    STREAMING_LOG(WARNING) << "metrics is disable";
    return;
  }
  perf_metrics_reporter_.reset(new StreamingReporter());

  std::unordered_map<std::string, std::string> default_tag_map = {
      {"role", NodeType_Name(config_.GetNodeType())},
      {"op_name", config_.GetOpName()},
      {"worker_name", config_.GetWorkerName()}};
  metrics_config_.SetMetricsGlobalTags(default_tag_map);

  perf_metrics_reporter_->Start(metrics_config_);
}

void RuntimeContext::ReportMetrics(
    const std::string &metric_name, double value,
    const std::unordered_map<std::string, std::string> &tags) {
  if (config_.GetMetricsEnable()) {
    perf_metrics_reporter_->UpdateGauge(metric_name, tags, value);
  }
}

void RuntimeContext::RunTimer() {
  AutoSpinLock lock(report_flag_);
  if (runtime_status_ != RuntimeStatus::Running) {
    STREAMING_LOG(WARNING) << "Run timer failed in state "
                           << static_cast<uint8_t>(runtime_status_);
    return;
  }
  STREAMING_LOG(INFO) << "Streaming metric timer called, interval="
                      << metrics_config_.GetMetricsReportInterval();
  if (async_io_.stopped()) {
    STREAMING_LOG(INFO) << "Async io stopped, return from timer reporting.";
    return;
  }
  this->report_timer_handler_();
  boost::posix_time::seconds interval(metrics_config_.GetMetricsReportInterval());
  metrics_timer_->expires_from_now(interval);
  metrics_timer_->async_wait([this](const boost::system::error_code &e) {
    if (boost::asio::error::operation_aborted == e) {
      return;
    }
    this->RunTimer();
  });
}

void RuntimeContext::EnableTimer(std::function<void()> report_timer_handler) {
  if (!config_.GetMetricsEnable()) {
    STREAMING_LOG(WARNING) << "Streaming metrics disabled.";
    return;
  }
  if (enable_timer_service_) {
    STREAMING_LOG(INFO) << "Timer service already enabled";
    return;
  }
  this->report_timer_handler_ = report_timer_handler;
  STREAMING_LOG(INFO) << "Streaming metric timer enabled";
  // We new a thread for timer if timer is not alive currently.
  if (!timer_thread_) {
    async_io_.reset();
    boost::posix_time::seconds interval(metrics_config_.GetMetricsReportInterval());
    metrics_timer_.reset(new boost::asio::deadline_timer(async_io_, interval));
    metrics_timer_->async_wait(
        [this](const boost::system::error_code & /*e*/) { this->RunTimer(); });
    timer_thread_ = std::make_shared<std::thread>([this]() {
      STREAMING_LOG(INFO) << "Async io running.";
      async_io_.run();
    });
    STREAMING_LOG(INFO) << "New thread " << timer_thread_->get_id();
  }
  enable_timer_service_ = true;
}

void RuntimeContext::ShutdownTimer() {
  {
    AutoSpinLock lock(report_flag_);
    if (!config_.GetMetricsEnable()) {
      STREAMING_LOG(WARNING) << "Streaming metrics disabled";
      return;
    }
    if (!enable_timer_service_) {
      STREAMING_LOG(INFO) << "Timer service already disabled";
      return;
    }
    STREAMING_LOG(INFO) << "Timer server shutdown";
    enable_timer_service_ = false;
    STREAMING_LOG(INFO) << "Cancel metrics timer.";
    metrics_timer_->cancel();
  }
  STREAMING_LOG(INFO) << "Wake up all reporting conditions.";
  if (timer_thread_) {
    STREAMING_LOG(INFO) << "Join and reset timer thread.";
    if (timer_thread_->joinable()) {
      timer_thread_->join();
    }
    timer_thread_.reset();
    metrics_timer_.reset();
  }
}

}  // namespace streaming
}  // namespace ray
