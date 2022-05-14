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

#include <thread>

#include "ray/common/status.h"
#include "ray/rpc/grpc_client.h"
#include "ray/util/logging.h"
#include "src/ray/protobuf/node_manager.grpc.pb.h"
#include "src/ray/protobuf/node_manager.pb.h"

namespace ray {
namespace rpc {

/// Client used for communicating with a remote node manager server.
class NodeManagerClient {
 public:
  /// Constructor.
  ///
  /// \param[in] address Address of the node manager server.
  /// \param[in] port Port of the node manager server.
  /// \param[in] client_call_manager The `ClientCallManager` used for managing requests.
  NodeManagerClient(const std::string &address,
                    const int port,
                    ClientCallManager &client_call_manager) {
    grpc_client_ = std::make_unique<GrpcClient<NodeManagerService>>(
        address, port, client_call_manager);
  };

  /// Get current node stats.
  VOID_RPC_CLIENT_METHOD(NodeManagerService,
                         GetNodeStats,
                         grpc_client_,
                         /*method_timeout_ms*/ -1, )

  void GetNodeStats(const ClientCallback<GetNodeStatsReply> &callback) {
    GetNodeStatsRequest request;
    GetNodeStats(request, callback);
  }

  std::shared_ptr<grpc::Channel> Channel() const { return grpc_client_->Channel(); }

 private:
  /// The RPC client.
  std::unique_ptr<GrpcClient<NodeManagerService>> grpc_client_;
};

/// Client used by workers for communicating with a node manager server.
class NodeManagerWorkerClient
    : public std::enable_shared_from_this<NodeManagerWorkerClient> {
 public:
  /// Constructor.
  ///
  /// \param[in] address Address of the node manager server.
  /// \param[in] port Port of the node manager server.
  /// \param[in] client_call_manager The `ClientCallManager` used for managing requests.
  static std::shared_ptr<NodeManagerWorkerClient> make(
      const std::string &address,
      const int port,
      ClientCallManager &client_call_manager) {
    auto instance = new NodeManagerWorkerClient(address, port, client_call_manager);
    return std::shared_ptr<NodeManagerWorkerClient>(instance);
  }

  std::shared_ptr<grpc::Channel> Channel() const { return grpc_client_->Channel(); }

  /// Update cluster resource usage.
  VOID_RPC_CLIENT_METHOD(NodeManagerService,
                         UpdateResourceUsage,
                         grpc_client_,
                         /*method_timeout_ms*/ -1, )

  /// Request a resource report.
  VOID_RPC_CLIENT_METHOD(NodeManagerService,
                         RequestResourceReport,
                         grpc_client_,
                         /*method_timeout_ms*/ -1, )

  /// Get a resource load
  VOID_RPC_CLIENT_METHOD(NodeManagerService,
                         GetResourceLoad,
                         grpc_client_,
                         /*method_timeout_ms*/ -1, )

  /// Request a worker lease.
  VOID_RPC_CLIENT_METHOD(NodeManagerService,
                         RequestWorkerLease,
                         grpc_client_,
                         /*method_timeout_ms*/ -1, )

  /// Report task backlog information
  VOID_RPC_CLIENT_METHOD(NodeManagerService,
                         ReportWorkerBacklog,
                         grpc_client_,
                         /*method_timeout_ms*/ -1, )

  /// Return a worker lease.
  VOID_RPC_CLIENT_METHOD(NodeManagerService,
                         ReturnWorker,
                         grpc_client_,
                         /*method_timeout_ms*/ -1, )

  /// Release unused workers.
  VOID_RPC_CLIENT_METHOD(NodeManagerService,
                         ReleaseUnusedWorkers,
                         grpc_client_,
                         /*method_timeout_ms*/ -1, )

  /// Shutdown the raylet gracefully.
  VOID_RPC_CLIENT_METHOD(NodeManagerService,
                         ShutdownRaylet,
                         grpc_client_,
                         /*method_timeout_ms*/ -1, )

  /// Cancel a pending worker lease request.
  VOID_RPC_CLIENT_METHOD(NodeManagerService,
                         CancelWorkerLease,
                         grpc_client_,
                         /*method_timeout_ms*/ -1, )

  /// Request prepare resources for an atomic placement group creation.
  VOID_RPC_CLIENT_METHOD(NodeManagerService,
                         PrepareBundleResources,
                         grpc_client_,
                         /*method_timeout_ms*/ -1, )

  /// Request commit resources for an atomic placement group creation.
  VOID_RPC_CLIENT_METHOD(NodeManagerService,
                         CommitBundleResources,
                         grpc_client_,
                         /*method_timeout_ms*/ -1, )

  /// Return resource lease.
  VOID_RPC_CLIENT_METHOD(NodeManagerService,
                         CancelResourceReserve,
                         grpc_client_,
                         /*method_timeout_ms*/ -1, )

  /// Notify the raylet to pin the provided object IDs.
  VOID_RPC_CLIENT_METHOD(NodeManagerService,
                         PinObjectIDs,
                         grpc_client_,
                         /*method_timeout_ms*/ -1, )

  /// Trigger global GC across the cluster.
  VOID_RPC_CLIENT_METHOD(NodeManagerService,
                         GlobalGC,
                         grpc_client_,
                         /*method_timeout_ms*/ -1, )

  /// Ask the raylet to spill an object to external storage.
  VOID_RPC_CLIENT_METHOD(NodeManagerService,
                         RequestObjectSpillage,
                         grpc_client_,
                         /*method_timeout_ms*/ -1, )

  /// Release unused bundles.
  VOID_RPC_CLIENT_METHOD(NodeManagerService,
                         ReleaseUnusedBundles,
                         grpc_client_,
                         /*method_timeout_ms*/ -1, )

  /// Get the system config from Raylet.
  VOID_RPC_CLIENT_METHOD(NodeManagerService,
                         GetSystemConfig,
                         grpc_client_,
                         /*method_timeout_ms*/ -1, )

  /// Get gcs server address.
  VOID_RPC_CLIENT_METHOD(NodeManagerService,
                         GetGcsServerAddress,
                         grpc_client_,
                         /*method_timeout_ms*/ -1, )

  /// Get all the task information from the node.
  VOID_RPC_CLIENT_METHOD(NodeManagerService,
                         GetTasksInfo,
                         grpc_client_,
                         /*method_timeout_ms*/ -1, )

  /// Get all the object information from the node.
  VOID_RPC_CLIENT_METHOD(NodeManagerService,
                         GetObjectsInfo,
                         grpc_client_,
                         /*method_timeout_ms*/ -1, )

 private:
  /// Constructor.
  ///
  /// \param[in] address Address of the node manager server.
  /// \param[in] port Port of the node manager server.
  /// \param[in] client_call_manager The `ClientCallManager` used for managing requests.
  NodeManagerWorkerClient(const std::string &address,
                          const int port,
                          ClientCallManager &client_call_manager) {
    grpc_client_ = std::make_unique<GrpcClient<NodeManagerService>>(
        address, port, client_call_manager);
  };

  /// The RPC client.
  std::unique_ptr<GrpcClient<NodeManagerService>> grpc_client_;
};

}  // namespace rpc
}  // namespace ray
