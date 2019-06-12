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

/// Client used for communicating with a remote node manager server.
class NodeManagerClient {
  using ForwardTaskCallback =
      std::function<void(const Status &status, const ForwardTaskReply &reply)>;

 public:
  /// Constructor.
  ///
  /// \param address Address of the node manager server.
  /// \param port Port of the node manager server.
  /// \param client_call_manager The `ClientCallManager` used for managing requests.
  NodeManagerClient(const std::string &address, const int port,
                    ClientCallManager &client_call_manager)
      : client_call_manager_(client_call_manager) {
    std::shared_ptr<grpc::Channel> channel = grpc::CreateChannel(
        address + ":" + std::to_string(port), grpc::InsecureChannelCredentials());
    stub_ = NodeManagerService::NewStub(channel);
  };

  void ForwardTask(const ForwardTaskRequest &request,
                   const ForwardTaskCallback &callback) {
    client_call_manager_.CreateCall<NodeManagerService, ForwardTaskRequest,
                                    ForwardTaskReply, ForwardTaskCallback>(stub_, request,
                                                                           callback);
  }

 private:
  std::unique_ptr<NodeManagerService::Stub> stub_;
  ClientCallManager &client_call_manager_;
};

}  // namespace ray

#endif  // RAY_RPC_NODE_MANAGER_CLIENT_H
