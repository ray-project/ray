// Copyright 2025 The Ray Authors.
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

#include <atomic>
#include <functional>
#include <limits>
#include <memory>
#include <string>
#include <utility>

#include "ray/common/ray_config.h"
#include "ray/rpc/grpc_client.h"
#include "ray/rpc/retryable_grpc_client.h"
#include "ray/util/logging.h"
#include "src/ray/protobuf/events_event_aggregator_service.grpc.pb.h"
#include "src/ray/protobuf/events_event_aggregator_service.pb.h"

namespace ray {
namespace rpc {
using ray::rpc::events::AddEventsReply;
using ray::rpc::events::AddEventsRequest;

/// The maximum number of retries to wait for the event aggregator server to be ready.
/// This setting allows for 30 seconds of retries.
constexpr int kEventAggregatorInitMaxRetries = 30;
constexpr int kEventAggregatorInitRetryDelayMs = 1000;

/// Client used for sending ray events to the event aggregator server in the dashboard
/// agent.
class EventAggregatorClient {
 public:
  virtual ~EventAggregatorClient() = default;

  virtual void AddEvents(AddEventsRequest &&request,
                         const ClientCallback<AddEventsReply> &callback) = 0;

  virtual void Connect(const int port) {}

  virtual void WaitForServerReady(
      std::function<void(const Status &)> init_exporter_fn) = 0;
};

class EventAggregatorClientImpl : public EventAggregatorClient {
 public:
  /// Constructor for deferred connection.
  /// Call Connect() later to establish the connection when the port is known.
  ///
  /// \param[in] client_call_manager The `ClientCallManager` used for managing requests.
  explicit EventAggregatorClientImpl(ClientCallManager &client_call_manager)
      : client_call_manager_(&client_call_manager),
        check_connection_interval_ms_(
            ::RayConfig::instance().ray_event_aggregator_check_connection_interval_ms()),
        reconnect_timeout_s_(
            ::RayConfig::instance().ray_event_aggregator_reconnect_timeout_s()),
        grpc_timeout_ms_(::RayConfig::instance().ray_event_recorder_grpc_timeout_ms()) {}

  /// Constructor with immediate connection.
  ///
  /// \param[in] port Port of the event aggregator server.
  /// \param[in] client_call_manager The `ClientCallManager` used for managing requests.
  EventAggregatorClientImpl(const int port, ClientCallManager &client_call_manager)
      : client_call_manager_(&client_call_manager),
        check_connection_interval_ms_(
            ::RayConfig::instance().ray_event_aggregator_check_connection_interval_ms()),
        reconnect_timeout_s_(
            ::RayConfig::instance().ray_event_aggregator_reconnect_timeout_s()),
        grpc_timeout_ms_(::RayConfig::instance().ray_event_recorder_grpc_timeout_ms()) {
    Connect(port);
  };

  void Connect(const int port) override {
    grpc_client_ = std::make_unique<GrpcClient<rpc::events::EventAggregatorService>>(
        "127.0.0.1", port, *client_call_manager_);

    // Create RetryableGrpcClient for automatic retry with exponential backoff
    retryable_grpc_client_ = RetryableGrpcClient::Create(
        grpc_client_->Channel(),
        client_call_manager_->GetMainService(),
        /*max_pending_requests_bytes=*/std::numeric_limits<uint64_t>::max(),
        /*check_channel_status_interval_milliseconds=*/
        check_connection_interval_ms_,
        /*server_reconnect_timeout_base_seconds=*/
        reconnect_timeout_s_,
        /*server_reconnect_timeout_max_seconds=*/
        reconnect_timeout_s_ * 2,
        /*server_unavailable_timeout_callback=*/
        []() { RAY_LOG(WARNING) << "Event aggregator unavailable for extended period"; },
        /*server_name=*/"Event aggregator");
  }

  void WaitForServerReady(std::function<void(const Status &)> init_exporter_fn) override {
    WaitForServerReadyWithRetry(std::move(init_exporter_fn),
                                0,
                                kEventAggregatorInitMaxRetries,
                                kEventAggregatorInitRetryDelayMs);
  }

  // Use VOID_RETRYABLE_RPC_CLIENT_METHOD for automatic retry with exponential backoff
  VOID_RETRYABLE_RPC_CLIENT_METHOD(retryable_grpc_client_,
                                   rpc::events::EventAggregatorService,
                                   AddEvents,
                                   grpc_client_,
                                   /*method_timeout_ms*/ grpc_timeout_ms_,
                                   override)

 private:
  virtual void CheckServerReady(const ClientCallback<AddEventsReply> &callback) {
    RAY_CHECK(grpc_client_ != nullptr)
        << "EventAggregatorClientImpl::Connect must be called before "
           "WaitForServerReady.";
    grpc_client_->CallMethod<AddEventsRequest, AddEventsReply>(
        &rpc::events::EventAggregatorService::Stub::PrepareAsyncAddEvents,
        AddEventsRequest(),
        callback,
        "EventAggregatorService.grpc_client.AddEvents",
        kEventAggregatorInitRetryDelayMs);
  }

  void WaitForServerReadyWithRetry(std::function<void(const Status &)> init_exporter_fn,
                                   int retry_count,
                                   int max_retry,
                                   int retry_interval_ms) {
    if (exporter_initialized_) {
      return;
    }

    if (retry_count == 0) {
      RAY_LOG(INFO) << "Initializing event aggregator client ...";
    }

    CheckServerReady(
        [this,
         init_exporter_fn = std::move(init_exporter_fn),
         retry_count,
         max_retry,
         retry_interval_ms](const Status &status, const AddEventsReply &reply) mutable {
          if (status.ok()) {
            bool expected = false;
            if (!exporter_initialized_.compare_exchange_strong(expected, true)) {
              return;
            }
            init_exporter_fn(status);
            RAY_LOG(INFO) << "Event aggregator client initialized.";
            return;
          }
          if (retry_count >= max_retry) {
            init_exporter_fn(Status::RpcError(
                "Running out of retries to initialize the event aggregator.", 14));
            return;
          }
          client_call_manager_->GetMainService().post(
              [this,
               init_exporter_fn = std::move(init_exporter_fn),
               retry_count,
               max_retry,
               retry_interval_ms]() mutable {
                WaitForServerReadyWithRetry(std::move(init_exporter_fn),
                                            retry_count + 1,
                                            max_retry,
                                            retry_interval_ms);
              },
              "EventAggregatorClient.WaitForServerReadyWithRetry",
              retry_interval_ms * 1000);
        });
  }

  // Saved for deferred connection.
  ClientCallManager *client_call_manager_;
  // Config values read once at construction time.
  int64_t check_connection_interval_ms_;
  int64_t reconnect_timeout_s_;
  int64_t grpc_timeout_ms_;
  // The RPC client.
  std::shared_ptr<GrpcClient<rpc::events::EventAggregatorService>> grpc_client_;
  // Retryable client for automatic retry with exponential backoff.
  std::shared_ptr<RetryableGrpcClient> retryable_grpc_client_;
  // Whether the client is initialized.
  std::atomic<bool> exporter_initialized_{false};

  friend class EventAggregatorClientTest;
  FRIEND_TEST(EventAggregatorClientTest, WaitForServerReadyWithRetrySuccess);
  FRIEND_TEST(EventAggregatorClientTest, WaitForServerReadyWithRetryFailure);
  FRIEND_TEST(EventAggregatorClientTest, ConcurrentCallbacksCallInitExporterFnOnlyOnce);
  FRIEND_TEST(EventAggregatorClientTest, ExhaustedRetriesReturnsFailure);
};

}  // namespace rpc
}  // namespace ray
