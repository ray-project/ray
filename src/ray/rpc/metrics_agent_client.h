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

#pragma once

#include <grpcpp/grpcpp.h>

#include <memory>
#include <string>
#include <thread>

#include "ray/common/status.h"
#include "ray/rpc/grpc_client.h"
#include "ray/util/logging.h"
#include "ray/util/network_util.h"
#include "src/ray/protobuf/reporter.grpc.pb.h"
#include "src/ray/protobuf/reporter.pb.h"

namespace ray {
namespace rpc {

/// The maximum number of retries to wait for the server to be ready.
/// This setting allows for 30 seconds of retries.
constexpr int kMetricAgentInitMaxRetries = 30;
constexpr int kMetricAgentInitRetryDelayMs = 1000;

/// Client used for communicating with a remote node manager server.
class MetricsAgentClient {
 public:
  virtual ~MetricsAgentClient() = default;

  /// Report open census protobuf metrics to metrics agent.
  ///
  /// \param[in] request The request message.
  /// \param[in] callback The callback function that handles reply.
  VOID_RPC_CLIENT_VIRTUAL_METHOD_DECL(ReporterService, ReportOCMetrics)

  /// Send a health check request to the metrics agent.
  ///
  /// \param[in] request The request message.
  /// \param[in] callback The callback function that handles reply.
  VOID_RPC_CLIENT_VIRTUAL_METHOD_DECL(ReporterService, HealthCheck)

  /// Initialize an exporter (e.g. metrics, events exporter).
  ///
  /// This function ensures that the server is ready to receive metrics before
  /// initializing the exporter. If the server is not ready, it will retry for
  /// a number of times.
  virtual void WaitForServerReady(
      std::function<void(const Status &)> init_exporter_fn) = 0;
};

class MetricsAgentClientImpl : public MetricsAgentClient {
 public:
  /// Constructor.
  ///
  /// \param[in] address Address of the metrics agent server.
  /// \param[in] port Port of the metrics agent server.
  /// \param[in] io_service The `instrumented_io_context` used for managing requests.
  /// \param[in] client_call_manager The `ClientCallManager` used for managing requests.
  MetricsAgentClientImpl(const std::string &address,
                         const int port,
                         instrumented_io_context &io_service,
                         rpc::ClientCallManager &client_call_manager) {
    RAY_LOG(DEBUG) << "Initiate the metrics client of address:"
                   << BuildAddress(address, port);
    grpc_client_ =
        std::make_unique<GrpcClient<ReporterService>>(address, port, client_call_manager);
    retry_timer_ = std::make_unique<boost::asio::steady_timer>(io_service);
  };

  VOID_RPC_CLIENT_METHOD(ReporterService,
                         ReportOCMetrics,
                         grpc_client_,
                         /*method_timeout_ms*/ -1,
                         override)

  VOID_RPC_CLIENT_METHOD(ReporterService,
                         HealthCheck,
                         grpc_client_,
                         /*method_timeout_ms*/ -1,
                         override)

  /// Wait for the server to be ready. Invokes the callback with the final readiness
  /// status of the server.
  void WaitForServerReady(std::function<void(const Status &)> init_exporter_fn) override;

 private:
  /// The RPC client.
  std::unique_ptr<GrpcClient<ReporterService>> grpc_client_;
  /// Timer for retrying to initialize the OpenTelemetry exporter.
  std::unique_ptr<boost::asio::steady_timer> retry_timer_;
  /// Whether the exporter is initialized.
  bool exporter_initialized_ = false;
  /// Wait for the server to be ready with a retry count. Invokes the callback
  /// with the status of the server. This is a helper function for WaitForServerReady.
  void WaitForServerReadyWithRetry(std::function<void(const Status &)> init_exporter_fn,
                                   int retry_count,
                                   int max_retry,
                                   int retry_interval_ms);

  friend class MetricsAgentClientTest;
  FRIEND_TEST(MetricsAgentClientTest, WaitForServerReadyWithRetrySuccess);
  FRIEND_TEST(MetricsAgentClientTest, WaitForServerReadyWithRetryFailure);
};

}  // namespace rpc
}  // namespace ray
