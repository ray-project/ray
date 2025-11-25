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
#include <utility>

#include "ray/util/logging.h"

namespace ray {
namespace rpc {

void MetricsAgentClientImpl::WaitForServerReady(
    std::function<void(const Status &)> init_exporter_fn) {
  WaitForServerReadyWithRetry(std::move(init_exporter_fn),
                              0,
                              kMetricAgentInitMaxRetries,
                              kMetricAgentInitRetryDelayMs);
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
              [this,
               init_exporter_fn = std::move(init_exporter_fn),
               retry_count,
               max_retry,
               retry_interval_ms](auto &status, auto &&reply) {
                if (status.ok()) {
                  if (exporter_initialized_) {
                    return;
                  }
                  init_exporter_fn(status);
                  exporter_initialized_ = true;
                  RAY_LOG(INFO) << "Exporter initialized.";
                  return;
                }
                if (retry_count >= max_retry) {
                  init_exporter_fn(Status::RpcError(
                      "Running out of retries to initialize the metrics agent.", 14));
                  return;
                }
                io_service_.post(
                    [this,
                     init_exporter_fn = std::move(init_exporter_fn),
                     retry_count,
                     max_retry,
                     retry_interval_ms]() {
                      WaitForServerReadyWithRetry(std::move(init_exporter_fn),
                                                  retry_count + 1,
                                                  max_retry,
                                                  retry_interval_ms);
                    },
                    "MetricsAgentClient.WaitForServerReadyWithRetry",
                    retry_interval_ms * 1000);
              });
}

}  // namespace rpc
}  // namespace ray
