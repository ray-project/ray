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

#include <grpcpp/grpcpp.h>
#include <grpcpp/resource_quota.h>
#include <grpcpp/support/channel_arguments.h>

#include <memory>
#include <string>
#include <vector>

#include "ray/common/status.h"
#include "ray/object_manager/grpc_client_manager.h"
#include "ray/object_manager_rpc_client/object_manager_client_interface.h"
#include "ray/rpc/grpc_client.h"
#include "ray/util/logging.h"
#include "src/ray/protobuf/object_manager.grpc.pb.h"
#include "src/ray/protobuf/object_manager.pb.h"

namespace ray {
namespace rpc {

/// Client used for communicating with a remote object manager server.
class ObjectManagerClient : public ObjectManagerClientInterface {
 public:
  /// Constructor.
  ///
  /// \param[in] address Address of the node manager server.
  /// \param[in] port Port of the node manager server.
  /// \param[in] client_call_manager The `ClientCallManager` used for managing requests.
  ObjectManagerClient(const std::string &address,
                      const int port,
                      ClientCallManager &client_call_manager)
      : grpc_client_manager_(
            std::make_unique<GrpcClientManagerImpl<ObjectManagerService>>(
                address, port, client_call_manager)) {}

  /// Push object to remote object manager
  ///
  /// \param request The request message.
  /// \param callback The callback function that handles reply from server
  VOID_RPC_CLIENT_METHOD(ObjectManagerService,
                         Push,
                         grpc_client_manager_->GetGrpcClient(),
                         /*method_timeout_ms*/ -1,
                         override)

  /// Pull object from remote object manager
  ///
  /// \param request The request message
  /// \param callback The callback function that handles reply from server
  VOID_RPC_CLIENT_METHOD(ObjectManagerService,
                         Pull,
                         grpc_client_manager_->GetGrpcClient(),
                         /*method_timeout_ms*/ -1,
                         override)

  /// Tell remote object manager to free objects
  ///
  /// \param request The request message
  /// \param callback  The callback function that handles reply
  VOID_RPC_CLIENT_METHOD(ObjectManagerService,
                         FreeObjects,
                         grpc_client_manager_->GetGrpcClient(),
                         /*method_timeout_ms*/ -1,
                         override)

 private:
  std::unique_ptr<GrpcClientManager<ObjectManagerService>> grpc_client_manager_;
};

}  // namespace rpc
}  // namespace ray
