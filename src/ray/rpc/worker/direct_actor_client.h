#ifndef RAY_RPC_DIRECT_ACTOR_CLIENT_H
#define RAY_RPC_DIRECT_ACTOR_CLIENT_H

#include <thread>

#include <grpcpp/grpcpp.h>

#include "ray/common/status.h"
#include "ray/rpc/asio_client.h"
#include "ray/rpc/client_call.h"
#include "ray/util/logging.h"
#include "src/ray/protobuf/direct_actor.grpc.pb.h"
#include "src/ray/protobuf/direct_actor.pb.h"

namespace ray {
namespace rpc {

/// Client used for communicating with a direct actor server.
class DirectActorClient {
 public:
  virtual ~DirectActorClient() {}
  /// Push a task.
  ///
  /// \param[in] request The request message.
  /// \param[in] callback The callback function that handles reply.
  /// \return if the rpc call succeeds
  virtual ray::Status PushTask(const PushTaskRequest &request,
                               const ClientCallback<PushTaskReply> &callback) = 0;
};

/// Grpc client for direct actor call.
class DirectActorGrpcClient : public DirectActorClient {
 public:
  /// Constructor.
  ///
  /// \param[in] address Address of the direct actor server.
  /// \param[in] port Port of the direct actor server.
  /// \param[in] client_call_manager The `ClientCallManager` used for managing requests.
  DirectActorGrpcClient(const std::string &address, const int port,
                        ClientCallManager &client_call_manager)
      : client_call_manager_(client_call_manager) {
    std::shared_ptr<grpc::Channel> channel = grpc::CreateChannel(
        address + ":" + std::to_string(port), grpc::InsecureChannelCredentials());
    stub_ = DirectActorService::NewStub(channel);
  };

  /// Push a task.
  ///
  /// \param[in] request The request message.
  /// \param[in] callback The callback function that handles reply.
  /// \return if the rpc call succeeds
  ray::Status PushTask(const PushTaskRequest &request,
                       const ClientCallback<PushTaskReply> &callback) override {
    auto call = client_call_manager_
                    .CreateCall<DirectActorService, PushTaskRequest, PushTaskReply>(
                        *stub_, &DirectActorService::Stub::PrepareAsyncPushTask, request,
                        callback);
    return call->GetStatus();
  }

 private:
  /// The gRPC-generated stub.
  std::unique_ptr<DirectActorService::Stub> stub_;

  /// The `ClientCallManager` used for managing requests.
  ClientCallManager &client_call_manager_;
};

/// Asio based RPC client for direct actor call.
class DirectActorAsioClient : public DirectActorClient {
 public:
  /// Constructor.
  ///
  /// \param[in] address Address of the direct actor server.
  /// \param[in] port Port of the direct actor server.
  /// \param[in] io_service The `io_service` to process reply messages.
  DirectActorAsioClient(const std::string &address, const int port,
                        boost::asio::io_service &io_service)
      : rpc_client_(RpcServiceType::DirectActorServiceType, address, port, io_service) {}

  /// Push a task.
  ///
  /// \param[in] request The request message.
  /// \param[in] callback The callback function that handles reply.
  /// \return if the rpc call succeeds
  ray::Status PushTask(const PushTaskRequest &request,
                       const ClientCallback<PushTaskReply> &callback) override {
    const TaskSpecification task_spec(request.task_spec());
    // For asio based direct actor call, we apply an optimization that we only
    // require a reply for a call when the `NumReturns` for the task is non-zero.
    // Here we compare task_spec.NumReturns() with 1 to account for the dummy object.
    return rpc_client_
        .CallMethod<PushTaskRequest, PushTaskReply, DirectActorServiceMessageType>(
            DirectActorServiceMessageType::PushTaskRequestMessage,
            DirectActorServiceMessageType::PushTaskReplyMessage, request, callback,
            /* requires_reply= */ task_spec.NumReturns() > 1);
  }

 private:
  AsioRpcClient rpc_client_;
};

}  // namespace rpc
}  // namespace ray

#endif  // RAY_RPC_DIRECT_ACTOR_CLIENT_H
