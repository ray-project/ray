#ifndef RAY_RPC_NODE_MANAGER_CLIENT_H
#define RAY_RPC_NODE_MANAGER_CLIENT_H

#include <thread>

#include <grpcpp/grpcpp.h>

#include "ray/common/status.h"
#include "ray/util/logging.h"
#include "src/ray/protobuf/test.grpc.pb.h"
#include "src/ray/protobuf/test.pb.h"

#include "ray/rpc/client_call.h"

namespace ray {
namespace rpc {

/// Client used for communicating with a remote node manager server.
class DebugTestClient {
 public:
  /// Constructor.
  ///
  /// \param[in] address Address of the test server.
  /// \param[in] port Port of the node manager server.
  /// \param[in] client_call_manager The `ClientCallManager` used for managing requests.
  DebugTestClient(const std::string &address, const int port,
                  ClientCallManager &client_call_manager)
      : client_call_manager_(client_call_manager) {
    std::shared_ptr<grpc::Channel> channel = grpc::CreateChannel(
        address + ":" + std::to_string(port), grpc::InsecureChannelCredentials());
    stub_ = DebugEchoService::NewStub(channel);
  };

  /// Send an echo message and expect for a reply message.
  ///
  /// \param[in] request The request message.
  /// \param[in] callback The callback function that handles reply.
  Status DebugEcho(const EchoRequest &request,
                   const ClientCallback<EchoReply> &callback) {
    auto call = client_call_manager_.CreateCall<DebugEchoService, EchoRequest, EchoReply>(
        *stub_, &DebugEchoService::Stub::PrepareAsyncDebugEcho, request, callback);
    return call->GetStatus();
  }

 private:
  /// The gRPC-generated stub.
  std::unique_ptr<DebugEchoService::Stub> stub_;

  /// The `ClientCallManager` used for managing requests.
  ClientCallManager &client_call_manager_;
};

}  // namespace rpc
}  // namespace ray

#endif  // RAY_RPC_NODE_MANAGER_CLIENT_H
