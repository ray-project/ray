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
};

class EventAggregatorClientImpl : public EventAggregatorClient {
 public:
  /// Constructor.
  ///
  /// \param[in] port Port of the event aggregator server.
  /// \param[in] client_call_manager The `ClientCallManager` used for managing requests.
  EventAggregatorClientImpl(const int port, ClientCallManager &client_call_manager) {
    RAY_LOG(INFO) << "Initiating the local event aggregator client with port: " << port;
    grpc_client_ = std::make_unique<GrpcClient<rpc::events::EventAggregatorService>>(
        "127.0.0.1", port, client_call_manager);
  };

  VOID_RPC_CLIENT_METHOD(rpc::events::EventAggregatorService,
                         AddEvents,
                         grpc_client_,
                         /*method_timeout_ms*/ -1,
                         override)

 private:
  // The RPC client.
  std::unique_ptr<GrpcClient<rpc::events::EventAggregatorService>> grpc_client_;
};

}  // namespace rpc
}  // namespace ray
