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
#include "src/ray/protobuf/agent_manager.grpc.pb.h"

namespace ray {
namespace rpc {

/// Client used for communicating with a remote agent manager server.
class AgentManagerClient {
 public:
  /// Constructor.
  ///
  /// \param[in] address Address of the agent manager server.
  /// \param[in] port Port of the agent manager server.
  /// \param[in] client_call_manager The `ClientCallManager` used for managing requests.
  AgentManagerClient(const std::string &address, const int port,
                     ClientCallManager &client_call_manager) {
    grpc_client_ = std::make_unique<GrpcClient<AgentManagerService>>(address, port,
                                                                     client_call_manager);
  };

  /// Register agent service to the agent manager server
  ///
  /// \param request The request message
  /// \param callback  The callback function that handles reply
  VOID_RPC_CLIENT_METHOD(AgentManagerService, RegisterAgent, grpc_client_, )

 private:
  /// The RPC client.
  std::unique_ptr<GrpcClient<AgentManagerService>> grpc_client_;
};

}  // namespace rpc
}  // namespace ray
