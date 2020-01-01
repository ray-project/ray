#ifndef RAY_RPC_GRPC_CLIENT_H
#define RAY_RPC_GRPC_CLIENT_H

#include <grpcpp/grpcpp.h>
#include <boost/asio.hpp>

#include "ray/common/grpc_util.h"
#include "ray/common/status.h"
#include "ray/rpc/client_call.h"

namespace ray {
namespace rpc {

// This macro wraps the logic to call a specific RPC method of a service,
// to make it easier to implement a new RPC client.
#define INVOKE_RPC_CALL(SERVICE, METHOD, request, callback, rpc_client) \
  ({                                                                    \
    rpc_client->CallMethod<METHOD##Request, METHOD##Reply>(             \
        &SERVICE::Stub::PrepareAsync##METHOD, request, callback);       \
  })

// Define a void RPC client method.
#define VOID_RPC_CLIENT_METHOD(SERVICE, METHOD, request, callback, rpc_client)   \
  void METHOD(const METHOD##Request &request,                                    \
              const ClientCallback<METHOD##Reply> &callback) {                   \
    RAY_UNUSED(INVOKE_RPC_CALL(SERVICE, METHOD, request, callback, rpc_client)); \
  }

// Define a RPC client method that returns ray::Status.
#define RPC_CLIENT_METHOD(SERVICE, METHOD, request, callback, rpc_client)   \
  ray::Status METHOD(const METHOD##Request &request,                        \
                     const ClientCallback<METHOD##Reply> &callback) {       \
    return INVOKE_RPC_CALL(SERVICE, METHOD, request, callback, rpc_client); \
  }

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
  /// \tparam Request Type of the request message.
  /// \tparam Reply Type of the reply message.
  ///
  /// \param[in] prepare_async_function Pointer to the gRPC-generated
  /// `FooService::Stub::PrepareAsyncBar` function.
  /// \param[in] request The request message.
  /// \param[in] callback The callback function that handles reply.
  ///
  /// \return Status.
  template <class Request, class Reply>
  ray::Status CallMethod(
      const PrepareAsyncFunction<GrpcService, Request, Reply> prepare_async_function,
      const Request &request, const ClientCallback<Reply> &callback) {
    auto call = client_call_manager_.CreateCall<GrpcService, Request, Reply>(
        *stub_, prepare_async_function, request, callback);
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