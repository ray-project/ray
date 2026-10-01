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
#include <utility>

#include "ray/rpc/grpc_client.h"
#include "ray/util/logging.h"
#include "ray/util/network_util.h"
#include "src/ray/protobuf/events_event_aggregator_service.grpc.pb.h"
#include "src/ray/protobuf/events_event_aggregator_service.pb.h"

namespace ray {
namespace rpc {
using ray::rpc::events::AddEventsReply;
using ray::rpc::events::AddEventsRequest;

/// Client used for sending ray events to the event aggregator server in the dashboard
/// agent.
class EventAggregatorClient {
 public:
  virtual ~EventAggregatorClient() = default;

  virtual void AddEvents(const rpc::events::AddEventsRequest &request,
                         const ClientCallback<rpc::events::AddEventsReply> &callback) = 0;

  virtual void Connect(const int port) {}
};

class EventAggregatorClientImpl : public EventAggregatorClient {
 public:
  /// Constructor for deferred connection.
  /// Call Connect() later to establish the connection when the port is known.
  ///
  /// \param[in] client_call_manager The `ClientCallManager` used for managing requests.
  explicit EventAggregatorClientImpl(ClientCallManager &client_call_manager)
      : client_call_manager_(&client_call_manager) {}

  /// Constructor with immediate connection.
  ///
  /// \param[in] port Port of the event aggregator server.
  /// \param[in] client_call_manager The `ClientCallManager` used for managing requests.
  EventAggregatorClientImpl(const int port, ClientCallManager &client_call_manager)
      : client_call_manager_(&client_call_manager) {
    Connect(port);
  };

  void Connect(const int port) override {
    grpc_client_ = std::make_unique<GrpcClient<rpc::events::EventAggregatorService>>(
        GetLocalhostIP(), port, *client_call_manager_);
  }

  void AddEvents(const rpc::events::AddEventsRequest &request,
                 const ClientCallback<rpc::events::AddEventsReply> &callback) override {
    if (grpc_client_ == nullptr) {
      // Connect() was never called, so there is no aggregator to talk to (e.g. a
      // minimal install where metrics_agent_port <= 0 selects the deferred-connection
      // constructor). Report failure through the callback so the caller's in-flight
      // bookkeeping unwinds, instead of dereferencing a null client.
      RAY_LOG_EVERY_N(WARNING, 100)
          << "EventAggregatorClient is not connected; dropping AddEvents request.";
      callback(Status::Disconnected("Event aggregator client is not connected."),
               rpc::events::AddEventsReply());
      return;
    }
    INVOKE_RPC_CALL(rpc::events::EventAggregatorService,
                    AddEvents,
                    request,
                    callback,
                    grpc_client_,
                    /*method_timeout_ms*/ -1);
  }

 private:
  // Saved for deferred connection.
  ClientCallManager *client_call_manager_;
  // The RPC client.
  std::unique_ptr<GrpcClient<rpc::events::EventAggregatorService>> grpc_client_;
};

}  // namespace rpc
}  // namespace ray
