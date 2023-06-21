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

#include "ray/common/asio/instrumented_io_context.h"
#include "ray/rpc/grpc_server.h"
#include "ray/rpc/server_call.h"
#include "src/ray/protobuf/runtime_env_agent.grpc.pb.h"
#include "src/ray/protobuf/runtime_env_agent.pb.h"

namespace ray {
namespace rpc {

#define RAY_RUNTIME_ENV_RPC_HANDLERS                                     \
  RPC_SERVICE_HANDLER(RuntimeEnvService, GetOrCreateRuntimeEnv, -1)      \
  RPC_SERVICE_HANDLER(RuntimeEnvService, DeleteRuntimeEnvIfPossible, -1) \
  RPC_SERVICE_HANDLER(RuntimeEnvService, GetRuntimeEnvsInfo, -1)

/// Implementations of the `RuntimeEnvService`, check interface in
/// `src/ray/protobuf/runtime_env_agent.proto`.
class RuntimeEnvServiceHandler {
 public:
  virtual void HandleGetOrCreateRuntimeEnv(GetOrCreateRuntimeEnvRequest request,
                                     GetOrCreateRuntimeEnvReply *reply,
                                     SendReplyCallback send_reply_callback) = 0;

  virtual void HandleDeleteRuntimeEnvIfPossible(DeleteRuntimeEnvIfPossibleRequest request,
                                          DeleteRuntimeEnvIfPossibleReply *reply,
                                          SendReplyCallback send_reply_callback) = 0;

  virtual void HandleGetRuntimeEnvsInfo(GetRuntimeEnvsInfoRequest request,
                                  GetRuntimeEnvsInfoReply *reply,
                                  SendReplyCallback send_reply_callback) = 0;
};

/// The `GrpcService` for `RuntimeEnvGrpcService`.
class RuntimeEnvGrpcService : public GrpcService {
 public:
  /// Construct a `RuntimeEnvGrpcService`.
  ///
  /// \param[in] port See `GrpcService`.
  /// \param[in] handler The service handler that actually handle the requests.
  RuntimeEnvGrpcService(instrumented_io_context &io_service,
                        RuntimeEnvServiceHandler &service_handler)
      : GrpcService(io_service), service_handler_(service_handler){};

 protected:
  grpc::Service &GetGrpcService() override { return service_; }

  void InitServerCallFactories(
      const std::unique_ptr<grpc::ServerCompletionQueue> &cq,
      std::vector<std::unique_ptr<ServerCallFactory>> *server_call_factories) override {
    RAY_RUNTIME_ENV_RPC_HANDLERS
  }

 private:
  /// The grpc async service object.
  RuntimeEnvService::AsyncService service_;
  /// The service handler that actually handle the requests.
  RuntimeEnvServiceHandler &service_handler_;
};

}  // namespace rpc
}  // namespace ray
