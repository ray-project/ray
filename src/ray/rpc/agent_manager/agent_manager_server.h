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

#include "ray/rpc/grpc_server.h"
#include "ray/rpc/server_call.h"
#include "src/ray/protobuf/agent_manager.grpc.pb.h"
#include "src/ray/protobuf/agent_manager.pb.h"

namespace ray {
namespace rpc {

#define RAY_AGENT_MANAGER_RPC_HANDLERS \
  RPC_SERVICE_HANDLER(AgentManagerService, RegisterAgent)

/// Implementations of the `AgentManagerGrpcService`, check interface in
/// `src/ray/protobuf/agent_manager.proto`.
class AgentManagerServiceHandler {
 public:
  virtual ~AgentManagerServiceHandler() = default;
  /// Handle a `RegisterAgent` request.
  /// The implementation can handle this request asynchronously. When handling is done,
  /// the `send_reply_callback` should be called.
  ///
  /// \param[in] request The request message.
  /// \param[out] reply The reply message.
  /// \param[in] send_reply_callback The callback to be called when the request is done.
  virtual void HandleRegisterAgent(const RegisterAgentRequest &request,
                                   RegisterAgentReply *reply,
                                   SendReplyCallback send_reply_callback) = 0;
};

/// The `GrpcService` for `AgentManagerGrpcService`.
class AgentManagerGrpcService : public GrpcService {
 public:
  /// Construct a `AgentManagerGrpcService`.
  ///
  /// \param[in] port See `GrpcService`.
  /// \param[in] handler The service handler that actually handle the requests.
  AgentManagerGrpcService(boost::asio::io_service &io_service,
                          AgentManagerServiceHandler &service_handler)
      : GrpcService(io_service), service_handler_(service_handler){};

 protected:
  grpc::Service &GetGrpcService() override { return service_; }

  void InitServerCallFactories(
      const std::unique_ptr<grpc::ServerCompletionQueue> &cq,
      std::vector<std::unique_ptr<ServerCallFactory>> *server_call_factories) override {
    RAY_AGENT_MANAGER_RPC_HANDLERS
  }

 private:
  /// The grpc async service object.
  AgentManagerService::AsyncService service_;
  /// The service handler that actually handle the requests.
  AgentManagerServiceHandler &service_handler_;
};

}  // namespace rpc
}  // namespace ray
