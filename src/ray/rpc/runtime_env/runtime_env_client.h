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

#include "ray/rpc/client_call.h"
#include "ray/rpc/grpc_client.h"
#include "src/ray/protobuf/runtime_env_agent.grpc.pb.h"

namespace ray {
namespace rpc {

class RuntimeEnvAgentClientInterface {
 public:
  virtual void GetOrCreateRuntimeEnv(
      const rpc::GetOrCreateRuntimeEnvRequest &request,
      const rpc::ClientCallback<rpc::GetOrCreateRuntimeEnvReply> &callback) = 0;
  virtual void DeleteRuntimeEnvIfPossible(
      const rpc::DeleteRuntimeEnvIfPossibleRequest &request,
      const rpc::ClientCallback<rpc::DeleteRuntimeEnvIfPossibleReply> &callback) = 0;
  virtual ~RuntimeEnvAgentClientInterface(){};
};

/// Client used for communicating with a remote runtime env agent server.
class RuntimeEnvAgentClient : public RuntimeEnvAgentClientInterface {
 public:
  /// Constructor.
  ///
  /// \param[in] address Address of the server.
  /// \param[in] port Port of the server.
  /// \param[in] client_call_manager The `ClientCallManager` used for managing requests.
  RuntimeEnvAgentClient(const std::string &address,
                        const int port,
                        ClientCallManager &client_call_manager) {
    grpc_client_ = std::make_unique<GrpcClient<RuntimeEnvService>>(
        address, port, client_call_manager);
  };

  /// Create runtime env.
  ///
  /// \param request The request message
  /// \param callback  The callback function that handles reply
  VOID_RPC_CLIENT_METHOD(RuntimeEnvService,
                         GetOrCreateRuntimeEnv,
                         grpc_client_,
                         /*method_timeout_ms*/ -1, )

  /// Delete URIs.
  ///
  /// \param request The request message
  /// \param callback  The callback function that handles reply
  VOID_RPC_CLIENT_METHOD(RuntimeEnvService,
                         DeleteRuntimeEnvIfPossible,
                         grpc_client_,
                         /*method_timeout_ms*/ -1, )

 private:
  /// The RPC client.
  std::unique_ptr<GrpcClient<RuntimeEnvService>> grpc_client_;
};

}  // namespace rpc
}  // namespace ray
