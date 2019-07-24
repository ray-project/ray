#ifndef RAY_RPC_NODE_MANAGER_CLIENT_H
#define RAY_RPC_NODE_MANAGER_CLIENT_H

#include <thread>

#include <grpcpp/grpcpp.h>

#include "ray/common/status.h"
#include "ray/rpc/client_call.h"
#include "ray/util/logging.h"
#include "src/ray/protobuf/test.grpc.pb.h"
#include "src/ray/protobuf/test.pb.h"

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

  /// Forward a task and its uncommitted lineage.
  ///
  /// \param[in] request The request message.
  /// \param[in] callback The callback function that handles reply.
  Status DebugEcho(const DebugEchoRequest &request,
                   const ClientCallback<DebugEchoReply> &callback) {
    auto call = client_call_manager_
                    .CreateCall<DebugEchoService, DebugEchoRequest, DebugEchoReply>(
                        *stub_, &DebugEchoService::Stub::PrepareAsyncDebugEcho, request,
                        callback);
    return call->GetStatus();
  }

  void StartEchoStream(const ClientCallback<Reply> &callback) {
    debug_stream_call_ =
        client_call_manager_
            .CreateStreamCall<DebugEchoService, DebugEchoRequest, DebugEchoReply>(
                *stub_, &DebugEchoService::Stub::AsyncDebugStreamEcho, callback);
  }

  /// Request for a stream message should be a synchronous call.
  void DebugStreamEcho(const DebugEchoRequest &request) {
    debug_stream_call_->WriteStream(request);
  }

  void CloseEchoStream() { debug_stream_call_->WritesDone(); }

 private:
  /// The gRPC-generated stub.
  std::unique_ptr<DebugEchoService::Stub> stub_;

  /// The `ClientCallManager` used for managing requests.
  ClientCallManager &client_call_manager_;

  /// The call for stream.
  std::shared_ptr<ClientCall<DebugEchoRequest, DebugEchoReply>> debug_stream_call_ =
      nullptr;
};

}  // namespace rpc
}  // namespace ray

#endif  // RAY_RPC_NODE_MANAGER_CLIENT_H
