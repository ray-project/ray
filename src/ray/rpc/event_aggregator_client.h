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

#include <memory>
#include <string>

#include "ray/rpc/grpc_client.h"
#include "ray/util/logging.h"
#include "src/ray/protobuf/event_aggregator_service.grpc.pb.h"
#include "src/ray/protobuf/event_aggregator_service.pb.h"

namespace ray {
namespace rpc {

/// Client used for communicating with an event aggregator server in the dashboard
/// agent.
class EventAggregatorClient {
 public:
  virtual ~EventAggregatorClient() = default;

  /// Report event to event aggregator.
  ///
  /// \param[in] request The request message.
  /// \param[in] callback The callback function that handles reply.
  virtual void AddEvents(const rpc::AddEventRequest &request,
                         const ClientCallback<rpc::AddEventReply> &callback) = 0;
};

class EventAggregatorClientImpl : public EventAggregatorClient {
 public:
  /// Constructor.
  ///
  /// \param[in] address Address of the event aggregator server.
  /// \param[in] port Port of the event aggregator server.
  /// \param[in] client_call_manager The `ClientCallManager` used for managing requests.
  EventAggregatorClientImpl(const std::string &address,
                            const int port,
                            ClientCallManager &client_call_manager) {
    RAY_LOG(INFO) << "Initiating the event aggregator client with address: " << address
                  << " port: " << port;
    grpc_client_ = std::make_unique<GrpcClient<EventAggregatorService>>(
        address, port, client_call_manager);
  };

  void AddEvents(const rpc::AddEventRequest &request,
                 const ClientCallback<rpc::AddEventReply> &callback) override {
    grpc_client_->CallMethod<rpc::AddEventRequest, rpc::AddEventReply>(
        &EventAggregatorService::Stub::PrepareAsyncReceiveEvents,
        request,
        callback,
        "EventAggregatorService.grpc_client.AddEvents",
        // TODO(myan): Add timeout and retry logic.
        /*timeout_ms*/ -1);
  }

 private:
  // The RPC client.
  std::unique_ptr<GrpcClient<EventAggregatorService>> grpc_client_;
};

}  // namespace rpc
}  // namespace ray
