#pragma once

#include <boost/asio.hpp>
#include <boost/thread/thread.hpp>
#include <string>

#include "common/status.h"
#include "config/streaming_config.h"
#include "metrics/streaming_perf_metric.h"

namespace ray {
namespace streaming {

enum class RuntimeStatus : uint8_t { Init = 0, Running = 1, Interrupted = 2 };

#define RETURN_IF_NOT_OK(STATUS_EXP)    \
  {                                     \
    StreamingStatus state = STATUS_EXP; \
    if (StreamingStatus::OK != state) { \
      return state;                     \
    }                                   \
  }

class RuntimeContext {
 public:
  RuntimeContext();
  virtual ~RuntimeContext();
  inline const StreamingConfig &GetConfig() const { return config_; };
  void SetConfig(const StreamingConfig &config);
  void SetConfig(const uint8_t *data, uint32_t buffer_len);
  inline RuntimeStatus GetRuntimeStatus() { return runtime_status_; }
  inline void SetRuntimeStatus(RuntimeStatus status) { runtime_status_ = status; }
  inline void MarkMockTest() { is_mock_test_ = true; }
  inline bool IsMockTest() { return is_mock_test_; }

  void InitMetricsReporter();
  void ReportMetrics(const std::string &metric_name, double value,
                     const std::map<std::string, std::string> &tags = {});
  void EnableTimer(std::function<void()> report_timer_handler);
  void ShutdownTimer();

 private:
  void RunTimer();

 protected:
  std::unique_ptr<StreamingPerf> perf_metrics_reporter_;
  std::function<void()> report_timer_handler_;

  boost::asio::io_service async_io_;

 private:
  bool enable_timer_service_;

  std::unique_ptr<boost::asio::deadline_timer> metrics_timer_;
  std::shared_ptr<std::thread> timer_thread_;
  std::atomic_flag report_flag_ = ATOMIC_FLAG_INIT;

  StreamingConfig config_;
  RuntimeStatus runtime_status_;
  StreamingMetricsConfig metrics_config_;
  bool is_mock_test_ = false;
};

}  // namespace streaming
}  // namespace ray
