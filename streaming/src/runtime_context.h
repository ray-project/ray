// Copyright 2021 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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

  ///  It's periodic reporter entry for all runtime modules.
  ///  \param metric_name, metric name
  ///  \param value, metric value
  ///  \param tags, metric tag map
  void ReportMetrics(const std::string &metric_name, double value,
                     const std::unordered_map<std::string, std::string> &tags = {});

  /// Enable and register a specific reporting timer for updating all of metrics.
  ///  \param reporter_timer_handler
  void EnableTimer(std::function<void()> report_timer_handler);

  /// Halt the timer invoking from now on.
  void ShutdownTimer();

 private:
  void RunTimer();

 protected:
  std::unique_ptr<StreamingReporter> perf_metrics_reporter_;
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
