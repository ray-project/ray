#ifndef RAY_RPC_GRPC_CLIENT_H
#define RAY_RPC_GRPC_CLIENT_H

#include <grpcpp/grpcpp.h>
#include <boost/asio.hpp>

#include "ray/common/grpc_util.h"
#include "ray/rpc/client_call.h"
#include "ray/common/status.h"

namespace ray {
namespace rpc {

// Call a specific rpc method.
#define RPC_CALL_METHOD(SERVICE, METHOD, request, callback)                           \
  RPC_CALL_METHOD_WITH_CLIENT(SERVICE, METHOD, request, callback, rpc_client_)        \

#define RPC_CALL_METHOD_WITH_CLIENT(SERVICE, METHOD, request, callback, rpc_client)   \
  ({                                                                                  \
      rpc_client->CallMethod<SERVICE, METHOD##Request, METHOD##Reply>(                \
                        &SERVICE::Stub::PrepareAsync##METHOD,                         \
                        request, callback);                                           \
  })                                                                                  \

template <class GrpcService>
class GrpcClient {
 public:
  GrpcClient(const std::string &address, const int port, ClientCallManager &call_manager)
    : client_call_manager_(call_manager) {
    std::shared_ptr<grpc::Channel> channel = grpc::CreateChannel(
        address + ":" + std::to_string(port), grpc::InsecureChannelCredentials());
    stub_ = GrpcService::NewStub(channel);
  }

  GrpcClient(const std::string &address, const int port, ClientCallManager &call_manager,
      int num_threads)
    : client_call_manager_(call_manager) {

    grpc::ResourceQuota quota;
    quota.SetMaxThreads(num_threads);
    grpc::ChannelArguments argument;
    argument.SetResourceQuota(quota);
    std::shared_ptr<grpc::Channel> channel =
        grpc::CreateCustomChannel(address + ":" + std::to_string(port),
                                  grpc::InsecureChannelCredentials(), argument);
    stub_ = GrpcService::NewStub(channel);
  }

  /// Create a new `ClientCall` and send request.
  ///
  /// \tparam GrpcService Type of the gRPC-generated service class.
  /// \tparam Request Type of the request message.
  /// \tparam Reply Type of the reply message.
  ///
  /// \param[in] stub The gRPC-generated stub.
  /// \param[in] prepare_async_function Pointer to the gRPC-generated
  /// `FooService::Stub::PrepareAsyncBar` function.
  /// \param[in] request The request message.
  /// \param[in] callback The callback function that handles reply.
  ///
  /// \return A `ClientCall` representing the request that was just sent.
  template <class Method, class Request, class Reply>
  ray::Status CallMethod(
      const PrepareAsyncFunction<GrpcService, Request, Reply> prepare_async_function,
      const Request &request, const ClientCallback<Reply> &callback) {

    auto call = client_call_manager_
                    .CreateCall<GrpcService, Request, Reply>(
                        *stub_, prepare_async_function, request,
                        callback);
    return call->GetStatus();    
  }
 private:

  ClientCallManager &client_call_manager_;
  /// The gRPC-generated stub.
  std::unique_ptr<typename GrpcService::Stub> stub_;
};

}  // namespace rpc
}  // namespace ray

#endif