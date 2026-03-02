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

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "ray/common/asio/instrumented_io_context.h"
#include "ray/rpc/authentication/authentication_token.h"
#include "ray/rpc/grpc_server.h"
#include "ray/rpc/raw_push_server_call.h"
#include "src/ray/protobuf/object_manager.grpc.pb.h"
#include "src/ray/protobuf/object_manager.pb.h"

namespace ray {
namespace rpc {

class ServerCallFactory;

#define RAY_OBJECT_MANAGER_RPC_SERVICE_HANDLER(METHOD) \
  RPC_SERVICE_HANDLER_CUSTOM_AUTH(                     \
      ObjectManagerService, METHOD, -1, ClusterIdAuthType::NO_AUTH)

// Only Pull and FreeObjects use standard protobuf handlers.
// Push uses raw ByteBuffer handling via RawPushServerCall.
#define RAY_OBJECT_MANAGER_RPC_HANDLERS        \
  RAY_OBJECT_MANAGER_RPC_SERVICE_HANDLER(Pull) \
  RAY_OBJECT_MANAGER_RPC_SERVICE_HANDLER(FreeObjects)

/// Implementations of the `ObjectManagerGrpcService`, check interface in
/// `src/ray/protobuf/object_manager.proto`.
class ObjectManagerServiceHandler : public RawPushHandler {
 public:
  /// Handle a raw `Push` request with pre-parsed header and data pointer.
  void HandlePush(PushRequest header,
                  const uint8_t *data,
                  size_t data_len,
                  SendReplyCallback send_reply_callback) override = 0;

  /// Handle a `Pull` request
  virtual void HandlePull(PullRequest request,
                          PullReply *reply,
                          SendReplyCallback send_reply_callback) = 0;
  /// Handle a `FreeObjects` request
  virtual void HandleFreeObjects(FreeObjectsRequest request,
                                 FreeObjectsReply *reply,
                                 SendReplyCallback send_reply_callback) = 0;
};

/// The `GrpcService` for `ObjectManagerGrpcService`.
class ObjectManagerGrpcService : public GrpcService {
 public:
  /// Construct a `ObjectManagerGrpcService`.
  ///
  /// \param[in] port See `GrpcService`.
  /// \param[in] handler The service handler that actually handle the requests.
  ObjectManagerGrpcService(instrumented_io_context &io_service,
                           ObjectManagerServiceHandler &service_handler)
      : GrpcService(io_service), service_handler_(service_handler){};

 protected:
  grpc::Service &GetGrpcService() override { return service_; }

  void InitServerCallFactories(
      const std::unique_ptr<grpc::ServerCompletionQueue> &cq,
      std::vector<std::unique_ptr<ServerCallFactory>> *server_call_factories,
      const ClusterID &cluster_id,
      std::shared_ptr<const AuthenticationToken> auth_token) override {
    // Register raw ByteBuffer handler for Push (method index 0).
    server_call_factories->emplace_back(std::make_unique<RawPushServerCallFactory>(
        service_,
        service_handler_,
        cq,
        main_service_,
        "ObjectManagerService.grpc_server.Push",
        /*record_metrics=*/true));

    // Register standard protobuf handlers for Pull and FreeObjects.
    RAY_OBJECT_MANAGER_RPC_HANDLERS
  }

 private:
  /// The grpc async service object (with Push marked as raw).
  RawObjectManagerAsyncService service_;
  /// The service handler that actually handle the requests.
  ObjectManagerServiceHandler &service_handler_;
};

}  // namespace rpc
}  // namespace ray
