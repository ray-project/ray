#ifndef RAY_RPC_NODE_MANAGER_CLIENT_H
#define RAY_RPC_NODE_MANAGER_CLIENT_H

#include <thread>

#include <grpcpp/grpcpp.h>

#include "ray/common/status.h"
#include "ray/rpc/client_call.h"
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
    std::shared_ptr<grpc::Channel> channel = grpc::CreateChannel(
        address + ":" + std::to_string(port), grpc::InsecureChannelCredentials());
    stub_ = NodeManagerService::NewStub(channel);
  };

  /// Forward a task and its uncommitted lineage.
  ///
  /// \param[in] request The request message.
  /// \param[in] callback The callback function that handles reply.
  void ForwardTask(const ForwardTaskRequest &request,
                   const ClientCallback<ForwardTaskReply> &callback) {
    client_call_manager_
        .CreateCall<NodeManagerService, ForwardTaskRequest, ForwardTaskReply>(
            *stub_, &NodeManagerService::Stub::PrepareAsyncForwardTask, request,
            callback);
  }

  /// Get current node stats.
  void GetNodeStats(const ClientCallback<NodeStatsReply> &callback) {
    NodeStatsRequest request;
    client_call_manager_.CreateCall<NodeManagerService, NodeStatsRequest, NodeStatsReply>(
        *stub_, &NodeManagerService::Stub::PrepareAsyncGetNodeStats, request, callback);
  }

 private:
  /// The gRPC-generated stub.
  std::unique_ptr<NodeManagerService::Stub> stub_;

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

  /// Submit a task.
  ray::Status SubmitTask(const SubmitTaskRequest &request,
                         const ClientCallback<SubmitTaskReply> &callback) {
    auto call = client_call_manager_
                    .CreateCall<NodeManagerService, SubmitTaskRequest, SubmitTaskReply>(
                        *stub_, &NodeManagerService::Stub::PrepareAsyncSubmitTask,
                        request, callback);
    return call->GetStatus();
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
    std::shared_ptr<grpc::Channel> channel = grpc::CreateChannel(
        address + ":" + std::to_string(port), grpc::InsecureChannelCredentials());
    stub_ = NodeManagerService::NewStub(channel);
  };

  /// The gRPC-generated stub.
  std::unique_ptr<NodeManagerService::Stub> stub_;

  /// The `ClientCallManager` used for managing requests.
  ClientCallManager &client_call_manager_;
};

}  // namespace rpc
}  // namespace ray

#endif  // RAY_RPC_NODE_MANAGER_CLIENT_H
