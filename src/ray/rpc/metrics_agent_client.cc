// Copyright 2017 The Ray Authors.
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

#include "ray/rpc/metrics_agent_client.h"

#include <chrono>
#include <functional>

#include "ray/util/logging.h"

namespace ray {
namespace rpc {

void MetricsAgentClientImpl::WaitForServerReady(
    std::function<void(const Status &)> init_exporter_fn) {
  WaitForServerReadyWithRetry(
      init_exporter_fn, 0, kMetricAgentInitMaxRetries, kMetricAgentInitRetryDelayMs);
}

void MetricsAgentClientImpl::WaitForServerReadyWithRetry(
    std::function<void(const Status &)> init_exporter_fn,
    int retry_count,
    int max_retry,
    int retry_interval_ms) {
  if (exporter_initialized_) {
    return;
  }

  if (retry_count == 0) {
    // Only log the first time we start the retry loop.
    RAY_LOG(INFO) << "Initializing exporter ...";
  }
  HealthCheck(rpc::HealthCheckRequest(),
              [this, init_exporter_fn](auto &status, auto &&reply) {
                if (status.ok() && !exporter_initialized_) {
                  init_exporter_fn(status);
                  exporter_initialized_ = true;
                  RAY_LOG(INFO) << "Exporter initialized.";
                }
              });
  if (retry_count >= max_retry) {
    init_exporter_fn(Status::RpcError("The metrics agent server is not ready.", 14));
    return;
  }
  retry_count++;
  retry_timer_->expires_after(std::chrono::milliseconds(retry_interval_ms));
  retry_timer_->async_wait(
      [this, init_exporter_fn, retry_count, max_retry, retry_interval_ms](
          const boost::system::error_code &error) {
        if (!error) {
          WaitForServerReadyWithRetry(
              init_exporter_fn, retry_count, max_retry, retry_interval_ms);
        } else {
          RAY_LOG(ERROR) << "Failed to initialize exporter. Data will not be exported to "
                            "the metrics agent.";
        }
      });
}

}  // namespace rpc
}  // namespace ray
