#ifndef RAY_RPC_GRPC_CLIENT_H
#define RAY_RPC_GRPC_CLIENT_H

#include <grpcpp/grpcpp.h>
#include <boost/asio.hpp>

#include "ray/common/grpc_util.h"
#include "ray/common/ray_config.h"
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
#define VOID_RPC_CLIENT_METHOD(SERVICE, METHOD, rpc_client, SPECS)               \
  void METHOD(const METHOD##Request &request,                                    \
              const ClientCallback<METHOD##Reply> &callback) SPECS {             \
    RAY_UNUSED(INVOKE_RPC_CALL(SERVICE, METHOD, request, callback, rpc_client)); \
  }

// Define a RPC client method that returns ray::Status.
#define RPC_CLIENT_METHOD(SERVICE, METHOD, rpc_client, SPECS)               \
  ray::Status METHOD(const METHOD##Request &request,                        \
                     const ClientCallback<METHOD##Reply> &callback) SPECS { \
    return INVOKE_RPC_CALL(SERVICE, METHOD, request, callback, rpc_client); \
  }

template <class GrpcService>
class GrpcClient {
 public:
  GrpcClient(const std::string &address, const int port, ClientCallManager &call_manager)
      : client_call_manager_(call_manager) {
    // Disable http proxy since it disrupts local connections. TODO(ekl) we should make
    // this configurable, or selectively set it for known local connections only.
    argument_.SetInt(GRPC_ARG_ENABLE_HTTP_PROXY, 0);
    argument_.SetMaxSendMessageSize(RayConfig::instance().max_grpc_message_size());
    argument_.SetMaxReceiveMessageSize(RayConfig::instance().max_grpc_message_size());
    std::shared_ptr<grpc::Channel> channel =
        grpc::CreateCustomChannel(address + ":" + std::to_string(port),
                                  grpc::InsecureChannelCredentials(), argument_);
    stub_ = GrpcService::NewStub(channel);
  }

  GrpcClient(const std::string &address, const int port, ClientCallManager &call_manager,
             const std::function<std::pair<std::string, int>()> &get_server_address)
      : client_call_manager_(call_manager) {
    argument_.SetInt(GRPC_ARG_ENABLE_HTTP_PROXY, 0);
    argument_.SetMaxSendMessageSize(RayConfig::instance().max_grpc_message_size());
    argument_.SetMaxReceiveMessageSize(RayConfig::instance().max_grpc_message_size());
    std::shared_ptr<grpc::Channel> channel =
        grpc::CreateCustomChannel(address + ":" + std::to_string(port),
                                  grpc::InsecureChannelCredentials(), argument_);
    stub_ = GrpcService::NewStub(channel);
    get_server_address_ = get_server_address;
  }

  GrpcClient(const std::string &address, const int port, ClientCallManager &call_manager,
             int num_threads)
      : client_call_manager_(call_manager) {
    grpc::ResourceQuota quota;
    quota.SetMaxThreads(num_threads);
    argument_.SetResourceQuota(quota);
    argument_.SetInt(GRPC_ARG_ENABLE_HTTP_PROXY, 0);
    argument_.SetMaxSendMessageSize(RayConfig::instance().max_grpc_message_size());
    argument_.SetMaxReceiveMessageSize(RayConfig::instance().max_grpc_message_size());
    std::shared_ptr<grpc::Channel> channel =
        grpc::CreateCustomChannel(address + ":" + std::to_string(port),
                                  grpc::InsecureChannelCredentials(), argument_);
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
    auto call = client_call_manager_.CreateCall<GrpcClient, GrpcService, Request, Reply>(
        this, prepare_async_function, request, callback);
    return call->GetStatus();
  }

  std::unique_ptr<typename GrpcService::Stub> &GetStub() {
    return stub_;
  }

  std::unique_ptr<typename GrpcService::Stub> &GetStubWithReconnect() {
    std::pair<std::string, int> address = get_server_address_();
    RAY_LOG(INFO) << "GetStub address = " << address.first << ", port = " << address.second;
    std::shared_ptr<grpc::Channel> channel =
        grpc::CreateCustomChannel(address.first + ":" + std::to_string(address.second),
                                  grpc::InsecureChannelCredentials(), argument_);
    stub_ = GrpcService::NewStub(channel);
    return stub_;
  }

 private:
  grpc::ChannelArguments argument_;
  ClientCallManager &client_call_manager_;
  /// The gRPC-generated stub.
  std::unique_ptr<typename GrpcService::Stub> stub_;

  /// This function is used to get rpc server address. Called only when reconnecting RPC server.
  std::function<std::pair<std::string, int>()> get_server_address_;
};

}  // namespace rpc
}  // namespace ray

#endif
