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
#include "ray/rpc/grpc_callback_server.h"
#include "src/ray/protobuf/agent_manager.grpc.pb.h"
#include "src/ray/protobuf/agent_manager.pb.h"

namespace ray {
namespace rpc {

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

#define RAY_AGENT_MANAGER_RPC_HANDLERS \
  UNARY_CALLBACK_RPC_SERVICE_HANDLER(AgentManagerService, RegisterAgent)

CALLBACK_SERVICE(AgentManagerService, RAY_AGENT_MANAGER_RPC_HANDLERS)

}  // namespace rpc
}  // namespace ray
