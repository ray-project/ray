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

#include <limits>
#include <memory>
#include <string>
#include <thread>
#include <utility>

#include "ray/common/status.h"
#include "ray/rpc/grpc_client.h"
#include "ray/rpc/retryable_grpc_client.h"
#include "ray/util/logging.h"
#include "src/ray/protobuf/node_manager.grpc.pb.h"
#include "src/ray/protobuf/node_manager.pb.h"

namespace ray {

namespace raylet {
class RayletClient;
}

namespace rpc {

/// TODO(dayshah): https://github.com/ray-project/ray/issues/54816 Kill this completely.
/// This class is only used by the RayletClient which is just a wrapper around this. This
/// exists for the legacy reason that all the function definitions in RayletClient have to
/// change if you move the things in here into RayletClient.
class NodeManagerClient {
 public:
  friend class raylet::RayletClient;

 private:
  /// Constructor.
  ///
  /// \param[in] address Address of the node manager server.
  /// \param[in] port Port of the node manager server.
  /// \param[in] client_call_manager The `ClientCallManager` used for managing requests.
  /// \param[in] raylet_unavailable_timeout_callback The callback function that is used
  /// by the retryable grpc to remove unresponsive raylet connections from the pool once
  /// its been unavailable for more than server_unavailable_timeout_seconds.
  NodeManagerClient(const rpc::Address &address,
                    ClientCallManager &client_call_manager,
                    std::function<void()> raylet_unavailable_timeout_callback)
      : grpc_client_(std::make_shared<GrpcClient<NodeManagerService>>(
            address.ip_address(), address.port(), client_call_manager)),
        retryable_grpc_client_(RetryableGrpcClient::Create(
            grpc_client_->Channel(),
            client_call_manager.GetMainService(),
            /*max_pending_requests_bytes=*/
            std::numeric_limits<uint64_t>::max(),
            /*check_channel_status_interval_milliseconds=*/
            ::RayConfig::instance()
                .grpc_client_check_connection_status_interval_milliseconds(),
            /*server_unavailable_timeout_seconds=*/
            ::RayConfig::instance().raylet_rpc_server_reconnect_timeout_s(),
            /*server_unavailable_timeout_callback=*/
            std::move(raylet_unavailable_timeout_callback),
            /*server_name=*/"Raylet " + address.ip_address())) {}

  std::shared_ptr<grpc::Channel> Channel() const { return grpc_client_->Channel(); }

  VOID_RPC_CLIENT_METHOD(NodeManagerService,
                         GetResourceLoad,
                         grpc_client_,
                         /*method_timeout_ms*/ -1, )

  VOID_RPC_CLIENT_METHOD(NodeManagerService,
                         CancelTasksWithResourceShapes,
                         grpc_client_,
                         /*method_timeout_ms*/ -1, )

  VOID_RPC_CLIENT_METHOD(NodeManagerService,
                         NotifyGCSRestart,
                         grpc_client_,
                         /*method_timeout_ms*/ -1, )

  VOID_RPC_CLIENT_METHOD(NodeManagerService,
                         RequestWorkerLease,
                         grpc_client_,
                         /*method_timeout_ms*/ -1, )

  VOID_RPC_CLIENT_METHOD(NodeManagerService,
                         PrestartWorkers,
                         grpc_client_,
                         /*method_timeout_ms*/ -1, )

  VOID_RPC_CLIENT_METHOD(NodeManagerService,
                         ReportWorkerBacklog,
                         grpc_client_,
                         /*method_timeout_ms*/ -1, )

  VOID_RPC_CLIENT_METHOD(NodeManagerService,
                         ReturnWorker,
                         grpc_client_,
                         /*method_timeout_ms*/ -1, )

  VOID_RPC_CLIENT_METHOD(NodeManagerService,
                         ReleaseUnusedActorWorkers,
                         grpc_client_,
                         /*method_timeout_ms*/ -1, )

  VOID_RPC_CLIENT_METHOD(NodeManagerService,
                         ShutdownRaylet,
                         grpc_client_,
                         /*method_timeout_ms*/ -1, )

  VOID_RPC_CLIENT_METHOD(NodeManagerService,
                         DrainRaylet,
                         grpc_client_,
                         /*method_timeout_ms*/ -1, )

  VOID_RPC_CLIENT_METHOD(NodeManagerService,
                         IsLocalWorkerDead,
                         grpc_client_,
                         /*method_timeout_ms*/ -1, )

  VOID_RPC_CLIENT_METHOD(NodeManagerService,
                         CancelWorkerLease,
                         grpc_client_,
                         /*method_timeout_ms*/ -1, )

  VOID_RPC_CLIENT_METHOD(NodeManagerService,
                         PrepareBundleResources,
                         grpc_client_,
                         /*method_timeout_ms*/ -1, )

  VOID_RPC_CLIENT_METHOD(NodeManagerService,
                         CommitBundleResources,
                         grpc_client_,
                         /*method_timeout_ms*/ -1, )

  VOID_RPC_CLIENT_METHOD(NodeManagerService,
                         CancelResourceReserve,
                         grpc_client_,
                         /*method_timeout_ms*/ -1, )

  VOID_RPC_CLIENT_METHOD(NodeManagerService,
                         PinObjectIDs,
                         grpc_client_,
                         /*method_timeout_ms*/ -1, )

  VOID_RPC_CLIENT_METHOD(NodeManagerService,
                         GlobalGC,
                         grpc_client_,
                         /*method_timeout_ms*/ -1, )

  VOID_RPC_CLIENT_METHOD(NodeManagerService,
                         ReleaseUnusedBundles,
                         grpc_client_,
                         /*method_timeout_ms*/ -1, )

  VOID_RPC_CLIENT_METHOD(NodeManagerService,
                         GetSystemConfig,
                         grpc_client_,
                         /*method_timeout_ms*/ -1, )

  /// Get all the object information from the node.
  VOID_RPC_CLIENT_METHOD(NodeManagerService,
                         GetObjectsInfo,
                         grpc_client_,
                         /*method_timeout_ms*/ -1, )

  VOID_RPC_CLIENT_METHOD(NodeManagerService,
                         GetTaskFailureCause,
                         grpc_client_,
                         /*method_timeout_ms*/ -1, )

  VOID_RPC_CLIENT_METHOD(NodeManagerService,
                         RegisterMutableObject,
                         grpc_client_,
                         /*method_timeout_ms*/ -1, )

  VOID_RPC_CLIENT_METHOD(NodeManagerService,
                         PushMutableObject,
                         grpc_client_,
                         /*method_timeout_ms*/ -1, )

  VOID_RPC_CLIENT_METHOD(NodeManagerService,
                         GetNodeStats,
                         grpc_client_,
                         /*method_timeout_ms*/ -1, )

  std::shared_ptr<GrpcClient<NodeManagerService>> grpc_client_;

  std::shared_ptr<RetryableGrpcClient> retryable_grpc_client_;
};

}  // namespace rpc
}  // namespace ray
