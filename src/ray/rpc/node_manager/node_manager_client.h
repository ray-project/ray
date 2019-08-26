#ifndef RAY_RPC_NODE_MANAGER_CLIENT_H
#define RAY_RPC_NODE_MANAGER_CLIENT_H

#include <thread>

#include <grpcpp/grpcpp.h>

#include "ray/common/status.h"
#include "ray/rpc/client_call.h"
#include "ray/rpc/asio_client.h"
#include "ray/util/logging.h"
#include "src/ray/protobuf/node_manager.grpc.pb.h"
#include "src/ray/protobuf/node_manager.pb.h"

namespace ray {
namespace rpc {

/// Client used for communicating with a remote node manager server.
class NodeManagerClient {
 public:
  /// Forward a task and its uncommitted lineage.
  ///
  /// \param[in] request The request message.
  /// \param[in] callback The callback function that handles reply.
  virtual void ForwardTask(const ForwardTaskRequest &request,
                   const ClientCallback<ForwardTaskReply> &callback) = 0;
};

/// Client used for communicating with a remote node manager server.
class NodeManagerGrpcClient : public NodeManagerClient {
 public:
  /// Constructor.
  ///
  /// \param[in] address Address of the node manager server.
  /// \param[in] port Port of the node manager server.
  /// \param[in] client_call_manager The `ClientCallManager` used for managing requests.
  NodeManagerGrpcClient(const std::string &address, const int port,
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
                   const ClientCallback<ForwardTaskReply> &callback) override {
    client_call_manager_
        .CreateCall<NodeManagerService, ForwardTaskRequest, ForwardTaskReply>(
            *stub_, &NodeManagerService::Stub::PrepareAsyncForwardTask, request,
            callback);
  }

 private:
  /// The gRPC-generated stub.
  std::unique_ptr<NodeManagerService::Stub> stub_;

  /// The `ClientCallManager` used for managing requests.
  ClientCallManager &client_call_manager_;
};

/// Asio based RPC client for direct actor call.
class NodeManagerAsioClient : public NodeManagerClient {
 public:
  /// Constructor.
  ///
  /// \param[in] address Address of the direct actor server.
  /// \param[in] port Port of the direct actor server.
  /// \param[in] io_service The `io_service` to process reply messages.
  NodeManagerAsioClient(const std::string &address, const int port,
                        boost::asio::io_service &io_service)
      : rpc_client_(RpcServiceType::NodeManagerServiceType, address, port, io_service) {}

  /// Push a task.
  ///
  /// \param[in] request The request message.
  /// \param[in] callback The callback function that handles reply.
  /// \return if the rpc call succeeds
  void ForwardTask(const ForwardTaskRequest &request,
                       const ClientCallback<ForwardTaskReply> &callback) override {
    rpc_client_
        .CallMethod<ForwardTaskRequest, ForwardTaskReply, NodeManagerServiceMessageType>(
            NodeManagerServiceMessageType::ForwardTaskRequestMessage,
            NodeManagerServiceMessageType::ForwardTaskReplyMessage, request, callback);
  }

 private:
  AsioRpcClient rpc_client_;
};

}  // namespace rpc
}  // namespace ray

#endif  // RAY_RPC_NODE_MANAGER_CLIENT_H
