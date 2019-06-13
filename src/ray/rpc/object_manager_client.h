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
class ObjectManagerClient {
  /*
  using ForwardTaskCallback =
      std::function<void(const Status &status, const ForwardTaskReply &reply)>;
  */

 public:
  /// Constructor.
  ///
  /// \param[in] address Address of the node manager server.
  /// \param[in] port Port of the node manager server.
  /// \param[in] client_call_manager The `ClientCallManager` used for managing requests.
  ObjectManagerClient(const std::string &address, const int port,
                    ClientCallManager &client_call_manager)
      : client_call_manager_(client_call_manager) {
    std::shared_ptr<grpc::Channel> channel = grpc::CreateChannel(
        address + ":" + std::to_string(port), grpc::InsecureChannelCredentials());
    stub_ = ObjectManagerService::NewStub(channel);
  };

  /// Forward a task and its uncommitted lineage.
  ///
  /// \param[in] request The request message.
  /// \param[in] callback The callback function that handles reply.
  void Push(const ForwardTaskRequest &request,
                   const ForwardTaskCallback &callback) {
    client_call_manager_.CreateCall<ObjectManagerService, PushRequest,
                                    PushReply, nullptr>(stub_, request, callback);
  }

 private:
  /// The gRPC-generated stub.
  std::unique_ptr<ObjectManagerService::Stub> stub_;

  /// The `ClientCallManager` used for managing requests.
  ClientCallManager &client_call_manager_;
};

}  // namespace ray

#endif  // RAY_RPC_NODE_MANAGER_CLIENT_H
