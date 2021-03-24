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
#include "src/ray/protobuf/job_agent.grpc.pb.h"

namespace ray {
namespace rpc {

/// Client used for communicating with a remote job agent server.
class JobClient {
 public:
  /// Constructor.
  ///
  /// \param[in] address Address of the job agent server.
  /// \param[in] port Port of the job agent server.
  /// \param[in] client_call_manager The `ClientCallManager` used for managing requests.
  JobClient(const std::string &address, const int port,
            ClientCallManager &client_call_manager)
      : client_call_manager_(client_call_manager) {
    grpc_client_.reset(
        new GrpcClient<JobAgentService>(address, port, client_call_manager_));
  };

  /// Initialize job env.
  ///
  /// \param request The request message
  /// \param callback  The callback function that handles reply
  VOID_RPC_CLIENT_METHOD(JobAgentService, InitializeJobEnv, grpc_client_, )

 private:
  /// The RPC client.
  std::unique_ptr<GrpcClient<JobAgentService>> grpc_client_;

  /// The `ClientCallManager` used for managing requests.
  ClientCallManager &client_call_manager_;
};

}  // namespace rpc
}  // namespace ray
