#ifndef RAY_RPC_NODE_MANAGER_CLIENT_H
#define RAY_RPC_NODE_MANAGER_CLIENT_H

#include <thread>

#include <grpcpp/grpcpp.h>

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
  NodeManagerClient(const std::string &address, const int port,
                    ClientCallManager &client_call_manager)
      : client_call_manager_(client_call_manager) {
    rpc_client_ = std::unique_ptr<GrpcClient<NodeManagerService>>(
        new GrpcClient<NodeManagerService>(address, port, client_call_manager));
  };

  /// Forward a task and its uncommitted lineage.
  ///
  /// \param[in] request The request message.
  /// \param[in] callback The callback function that handles reply.
  void ForwardTask(const ForwardTaskRequest &request,
                   const ClientCallback<ForwardTaskReply> &callback) {
    RPC_CALL_METHOD(NodeManagerService, ForwardTask, request, callback);
  }

  /// Get current node stats.
  void GetNodeStats(const ClientCallback<GetNodeStatsReply> &callback) {
    GetNodeStatsRequest request;
    RPC_CALL_METHOD(NodeManagerService, GetNodeStats, request, callback);
  }

 private:
  /// The RPC client.
  std::unique_ptr<GrpcClient<NodeManagerService>> rpc_client_;

  /// The `ClientCallManager` used for managing requests.
  ClientCallManager &client_call_manager_;
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
      const std::string &address, const int port,
      ClientCallManager &client_call_manager) {
    auto instance = new NodeManagerWorkerClient(address, port, client_call_manager);
    return std::shared_ptr<NodeManagerWorkerClient>(instance);
  }

  /// Request a worker lease.
  ray::Status RequestWorkerLease(
      const RequestWorkerLeaseRequest &request,
      const ClientCallback<RequestWorkerLeaseReply> &callback) {
    return RPC_CALL_METHOD(NodeManagerService, RequestWorkerLease, request, callback);
  }

  ray::Status ReturnWorker(const ReturnWorkerRequest &request,
                           const ClientCallback<ReturnWorkerReply> &callback) {
    return RPC_CALL_METHOD(NodeManagerService, ReturnWorker, request, callback);
  }

 private:
  /// Constructor.
  ///
  /// \param[in] address Address of the node manager server.
  /// \param[in] port Port of the node manager server.
  /// \param[in] client_call_manager The `ClientCallManager` used for managing requests.
  NodeManagerWorkerClient(const std::string &address, const int port,
                          ClientCallManager &client_call_manager)
      : client_call_manager_(client_call_manager) {
    rpc_client_ = std::unique_ptr<GrpcClient<NodeManagerService>>(
        new GrpcClient<NodeManagerService>(address, port, client_call_manager));
  };

  /// The RPC client.
  std::unique_ptr<GrpcClient<NodeManagerService>> rpc_client_;

  /// The `ClientCallManager` used for managing requests.
  ClientCallManager &client_call_manager_;
};

}  // namespace rpc
}  // namespace ray

#endif  // RAY_RPC_NODE_MANAGER_CLIENT_H
