// Copyright 2017 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <grpcpp/grpcpp.h>

#include <boost/asio.hpp>
#include <memory>
#include <string>
#include <string_view>
#include <utility>

#include "ray/common/grpc_util.h"
#include "ray/common/ray_config.h"
#include "ray/common/status.h"
#include "ray/rpc/client_call.h"
#include "ray/rpc/common.h"
#include "ray/rpc/rpc_chaos.h"
#include "ray/util/network_util.h"

namespace ray {
namespace rpc {

// This macro wraps the logic to call a specific RPC method of a service,
// to make it easier to implement a new RPC client.
#define INVOKE_RPC_CALL(                                               \
    SERVICE, METHOD, request, callback, rpc_client, method_timeout_ms) \
  (rpc_client->CallMethod<METHOD##Request, METHOD##Reply>(             \
      &SERVICE::Stub::PrepareAsync##METHOD,                            \
      request,                                                         \
      callback,                                                        \
      #SERVICE ".grpc_client." #METHOD,                                \
      method_timeout_ms))

// Define a void RPC client method declaration
#define VOID_RPC_CLIENT_VIRTUAL_METHOD_DECL(SERVICE, METHOD) \
  virtual void METHOD(const METHOD##Request &request,        \
                      const ClientCallback<METHOD##Reply> &callback) = 0;

// Define a void RPC client method.
#define VOID_RPC_CLIENT_METHOD(SERVICE, METHOD, rpc_client, method_timeout_ms, SPECS)   \
  void METHOD(const METHOD##Request &request,                                           \
              const ClientCallback<METHOD##Reply> &callback) SPECS {                    \
    INVOKE_RPC_CALL(SERVICE, METHOD, request, callback, rpc_client, method_timeout_ms); \
  }

/// Build a gRPC channel to the specified address.
///
/// This is the ONLY recommended way to create gRPC channels in Ray.
/// Use of raw grpc::CreateCustomChannel() should be avoided.
///
/// Authentication tokens are automatically added in metadata when RAY_AUTH_MODE=token.
///
/// \param address The server address
/// \param port The server port
/// \param arguments Optional channel arguments for customization
/// \return A shared pointer to the gRPC channel
std::shared_ptr<grpc::Channel> BuildChannel(
    const std::string &address,
    int port,
    std::optional<grpc::ChannelArguments> arguments = std::nullopt);

template <class GrpcService>
class GrpcClient {
 public:
  GrpcClient(std::shared_ptr<grpc::Channel> channel,
             ClientCallManager &call_manager,
             std::string_view server_address)
      : client_call_manager_(call_manager),
        channel_(std::move(channel)),
        stub_(GrpcService::NewStub(channel_)),
        skip_testing_intra_node_rpc_failure_(
            ::RayConfig::instance().testing_rpc_failure_avoid_intra_node_failures() &&
            IsLocalHost(server_address, call_manager.GetLocalAddress())) {}

  GrpcClient(const std::string &address,
             const int port,
             ClientCallManager &call_manager,
             grpc::ChannelArguments channel_arguments = CreateDefaultChannelArguments())
      : client_call_manager_(call_manager),
        channel_(BuildChannel(address, port, std::move(channel_arguments))),
        stub_(GrpcService::NewStub(channel_)),
        skip_testing_intra_node_rpc_failure_(
            ::RayConfig::instance().testing_rpc_failure_avoid_intra_node_failures() &&
            IsLocalHost(address, call_manager.GetLocalAddress())) {}

  /// Create a new `ClientCall` and send request.
  ///
  /// \tparam Request Type of the request message.
  /// \tparam Reply Type of the reply message.
  ///
  /// \param[in] prepare_async_function Pointer to the gRPC-generated
  /// `FooService::Stub::PrepareAsyncBar` function.
  /// \param[in] request The request message.
  /// \param[in] callback The callback function that handles reply.
  /// \param[in] call_name The name of the gRPC method call.
  /// \param[in] method_timeout_ms The timeout of the RPC method in ms.
  /// -1 means it will use the default timeout configured for the handler.
  ///
  /// \return Status.
  template <class Request, class Reply>
  void CallMethod(
      const PrepareAsyncFunction<GrpcService, Request, Reply> prepare_async_function,
      const Request &request,
      const ClientCallback<Reply> &callback,
      std::string call_name = "UNKNOWN_RPC",
      int64_t method_timeout_ms = -1) {
    testing::RpcFailure failure = skip_testing_intra_node_rpc_failure_
                                      ? testing::RpcFailure::None
                                      : testing::GetRpcFailure(call_name);
    if (failure == testing::RpcFailure::Request) {
      // Simulate the case where the PRC fails before server receives
      // the request.
      RAY_LOG(INFO) << "Inject RPC request failure for " << call_name;
      client_call_manager_.GetMainService().post(
          [callback]() {
            callback(Status::RpcError("Unavailable", grpc::StatusCode::UNAVAILABLE),
                     Reply());
          },
          "RpcChaos");
    } else if (failure == testing::RpcFailure::Response) {
      // Simulate the case where the RPC fails after server sends
      // the response.
      RAY_LOG(INFO) << "Inject RPC response failure for " << call_name;
      client_call_manager_.CreateCall<GrpcService, Request, Reply>(
          *stub_,
          prepare_async_function,
          request,
          [callback](const Status &status, const Reply &) {
            callback(Status::RpcError("Unavailable", grpc::StatusCode::UNAVAILABLE),
                     Reply());
          },
          std::move(call_name),
          method_timeout_ms);
    } else {
      auto call = client_call_manager_.CreateCall<GrpcService, Request, Reply>(
          *stub_,
          prepare_async_function,
          request,
          callback,
          std::move(call_name),
          method_timeout_ms);
      RAY_CHECK(call != nullptr);
    }

    call_method_invoked_.store(true);
  }

  std::shared_ptr<grpc::Channel> Channel() const { return channel_; }

  /// A channel is IDLE when it's first created before making any RPCs
  /// or after GRPC_ARG_CLIENT_IDLE_TIMEOUT_MS of no activities since the last RPC.
  /// This method detects IDLE in the second case.
  /// Also see https://grpc.github.io/grpc/core/md_doc_connectivity-semantics-and-api.html
  /// for channel connectivity state machine.
  bool IsChannelIdleAfterRPCs() const {
    return (channel_->GetState(false) == GRPC_CHANNEL_IDLE) &&
           call_method_invoked_.load();
  }

 private:
  ClientCallManager &client_call_manager_;
  /// The channel of the stub.
  std::shared_ptr<grpc::Channel> channel_;
  /// The gRPC-generated stub.
  std::unique_ptr<typename GrpcService::Stub> stub_;
  /// Whether CallMethod is invoked.
  std::atomic<bool> call_method_invoked_ = false;
  bool skip_testing_intra_node_rpc_failure_ = false;
};

}  // namespace rpc
}  // namespace ray
