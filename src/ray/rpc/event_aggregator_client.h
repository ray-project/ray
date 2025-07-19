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

#include "ray/common/status.h"
#include "ray/rpc/grpc_client.h"
#include "ray/util/logging.h"
#include "src/ray/protobuf/events_event_aggregator_service.grpc.pb.h"
#include "src/ray/protobuf/events_event_aggregator_service.pb.h"

namespace ray {
namespace rpc {

/// Client used for sending ray events to the event aggregator server in the dashboard
/// agent.
class EventAggregatorClient {
 public:
  virtual ~EventAggregatorClient() = default;

  virtual Status AsyncAddRayEventsData(
      std::unique_ptr<rpc::events::RayEventsData> data_ptr,
      std::function<void(Status status)> callback) = 0;
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

  Status AsyncAddRayEventsData(std::unique_ptr<rpc::events::RayEventsData> data_ptr,
                               std::function<void(Status status)> callback) override {
    rpc::events::AddEventRequest request;
    *request.mutable_events_data() = std::move(*data_ptr);
    AddEvents(request,
              [callback](const Status &status, const rpc::events::AddEventReply &reply) {
                callback(status);
                if (!status.ok()) {
                  RAY_LOG(DEBUG)
                      << "Failed to add Ray events to the event aggregator. Status: "
                      << status;
                }
              });

    return Status::OK();
  }

 private:
  void AddEvents(const rpc::events::AddEventRequest &request,
                 const ClientCallback<rpc::events::AddEventReply> &callback) {
    grpc_client_->CallMethod<rpc::events::AddEventRequest, rpc::events::AddEventReply>(
        &rpc::events::EventAggregatorService::Stub::PrepareAsyncAddEvents,
        request,
        callback,
        "EventAggregatorService.grpc_client.AddEvents",
        // TODO(myan): Add timeout and retry logic.
        /*timeout_ms*/ -1);
  }

  // The RPC client.
  std::unique_ptr<GrpcClient<rpc::events::EventAggregatorService>> grpc_client_;
};

}  // namespace rpc
}  // namespace ray
