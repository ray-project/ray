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
#include <grpcpp/impl/codegen/client_callback.h>

#include <boost/asio.hpp>

#include "ray/common/grpc_util.h"
#include "ray/common/ray_config.h"
#include "ray/common/status.h"
#include "ray/rpc/client_call.h"

namespace ray {
namespace rpc {

using ReplyCallback = std::function<void(grpc::Status)>;

template <class GrpcService, class Request, class Reply>
using AsyncUnaryMethod = void (GrpcService::StubInterface::async_interface::*)(
    grpc::ClientContext *context, const Request *request, Reply *response, ReplyCallback);

template <class GrpcService, class Request, class Reply>
using AsyncClientStreamingMethod = void (GrpcService::StubInterface::async_interface::*)(
    grpc::ClientContext *context, Reply *response,
    grpc::ClientWriteReactor<Request> *reactor);

template <class GrpcService, class Request, class Reply>
using AsyncServerStreamingMethod = void (GrpcService::StubInterface::async_interface::*)(
    grpc::ClientContext *context, Request *request,
    grpc::ClientReadReactor<Reply> *reactor);

template <class GrpcService, class Request, class Reply>
using AsyncBidiStreamingMethod = void (GrpcService::StubInterface::async_interface::*)(
    grpc::ClientContext *context, grpc::ClientBidiReactor<Request, Reply> *reactor);

// Define a unary RPC client method.
#define UNARY_CALLBACK_RPC_CLIENT_METHOD(SERVICE, METHOD, rpc_client, SPECS)    \
  void METHOD(const METHOD##Request &request,                                   \
              const ClientCallback<METHOD##Reply> &callback,                    \
              std::shared_ptr<grpc::ClientContext> ctx = nullptr) SPECS {       \
    rpc_client->CallUnaryMethod<METHOD##Request, METHOD##Reply>(                \
        static_cast<AsyncUnaryMethod<SERVICE, METHOD##Request, METHOD##Reply>>( \
            &SERVICE::Stub::async::METHOD),                                     \
        request, callback, ctx, #SERVICE ".grpc_client." #METHOD);              \
  }

// Define a client-side streaming RPC client method that requires a reactor.
#define CLIENT_STREAMING_REACTOR_RPC_CLIENT_METHOD(METHOD, rpc_client, SPECS) \
  void METHOD(grpc::ClientContext *ctx, METHOD##Reply *reply,                 \
              grpc::ClientWriteReactor<METHOD##Request> reactor) SPECS {      \
    return rpc_client->GetStub()->async()->METHOD(ctx, reply, reactor);       \
  }

// Define a server-side streaming RPC client method that requires a reactor.
#define SERVER_STREAMING_REACTOR_RPC_CLIENT_METHOD(METHOD, rpc_client, SPECS) \
  void METHOD(grpc::ClientContext *ctx, METHOD##Request *request,             \
              grpc::ClientReadReactor<METHOD##Reply> reactor) SPECS {         \
    return rpc_client->GetStub()->async()->METHOD(ctx, request, reactor);     \
  }

// Define a bidi streaming RPC client method that requires a reactor.
#define BIDI_STREAMING_REACTOR_RPC_CLIENT_METHOD(METHOD, rpc_client, SPECS)            \
  void METHOD(grpc::ClientContext *ctx,                                                \
              grpc::ClientBidiReactor<METHOD##Request, METHOD##Reply> reactor) SPECS { \
    return rpc_client->GetStub()->sync()->METHOD(ctx, reactor);                        \
  }

///
/// Experimental callback-only APIs on top of streaming reactors.
///

template <class GrpcService>
class GrpcCallbackClient;

/// A client reactor for client-side streaming RPCs that invokes a user-provided callback
/// with the RPC response and status upon receipt, the latter of which is converted from
/// a gRPC status to a Ray status.
///
/// NOTE: This class must be allocated on the heap, and will self-delete after the
/// user-provided callback is invoked.
template <class GrpcService, class Request, class Reply>
class ClientWriterReactor : public grpc::ClientWriteReactor<Request> {
 public:
  ClientWriterReactor(ClientCallback<Reply> callback, const std::string call_name,
                      instrumented_io_context &io_context)
      : callback_(std::move(callback)),
        call_name_(std::move(call_name)),
        io_context_(io_context) {}

  void OnDone(const grpc::Status &status) {
    io_context_.post(
        [this, status]() {
          callback_(GrpcStatusToRayStatus(status), response_);
          // The reactor is guaranteed to not be used by gRPC after OnDone() is called, so
          // we can delete this reactor. NOTE: This assumes that the reactor was allocated
          // on the heap, and that the application code will not use the reactor after
          // OnDone() is called.
          delete this;
        },
        call_name_);
  }

 private:
  /// The RPC response that the callback will eventually receive. This will be filled by
  /// the gRPC library upon receipt of the response.
  Reply response_;
  /// The user-provided callback, taking a Ray status and an RPC response.
  ClientCallback<Reply> callback_;
  /// A human-readable name for this RPC method.
  std::string call_name_;
  /// The event loop to which the response callback will be posted.
  instrumented_io_context &io_context_;

  friend class GrpcCallbackClient<GrpcService>;
};

/// A user-friendly client interface for writing to a client-side streaming RPC.
/// This interface wraps the less user-friendly ClientWriterReactor that the user would
/// otherwise need to derive from.
template <class GrpcService, class Request, class Reply>
class ClientStreamingWriter {
 public:
  ClientStreamingWriter(
      ClientWriterReactor<GrpcService, Request, Reply> *client_writer_reactor)
      : client_writer_reactor_(client_writer_reactor) {
    RAY_CHECK(client_writer_reactor_ != nullptr);
    // We add a hold to the reactor, which is required if writes are going to originate
    // outside of reactions, e.g. via the Write() method we expose to application-level
    // code.
    client_writer_reactor_->AddHold();
    has_hold_ = true;
    // NOTE: The hold must be added before the call is started.
    client_writer_reactor_->StartCall();
  }

  ~ClientStreamingWriter() {
    if (has_hold_) {
      // NOTE: The reactor will never call OnDone(), and therefore the user-provided RPC
      // response processing callback will never be invoked, unless we remove all added
      // holds.
      client_writer_reactor_->RemoveHold();
      has_hold_ = false;
    }
  }

  /// Send a request on the client-streaming RPC to the server.
  ///
  /// \param[in] request The request message that will be sent.
  void Write(const Request &request) { client_writer_reactor_->StartWrite(&request); }

  /// Send the last request on the client-streaming RPC to the server. This is
  /// equivalent to calling Write() followed by DoneWriting(), although WriteLast() is
  /// more efficient.
  ///
  /// NOTE: Only one of WriteLast() and DoneWriting() should be called.
  ///
  /// \param[in] request The request message that will be sent.
  void WriteLast(const Request &request) {
    client_writer_reactor_->StartWriteLast(&request);
    RAY_CHECK(has_hold_);
    client_writer_reactor_->RemoveHold();
    has_hold_ = false;
  }

  /// Indicate that we are done sending requests.
  ///
  /// NOTE: Only one of WriteLast() and DoneWriting() should be called.
  void DoneWriting() {
    client_writer_reactor_->StartWritesDone();
    RAY_CHECK(has_hold_);
    client_writer_reactor_->RemoveHold();
    has_hold_ = false;
  }

 private:
  /// The client reactor for client-side streaming RPCs that this interface wraps.
  ClientWriterReactor<GrpcService, Request, Reply> *client_writer_reactor_;
  /// Whether this writer current possesses a hold on the reactor.
  bool has_hold_;
};

// Define a client-side streaming RPC client method.
#define CLIENT_STREAMING_CALLBACK_RPC_CLIENT_METHOD(SERVICE, METHOD, rpc_client, SPECS) \
  ClientStreamingWriter<SERVICE, METHOD##Request, METHOD##Reply> METHOD(                \
      const ClientCallback<METHOD##Reply> &callback,                                    \
      std::shared_ptr<grpc::ClientContext> ctx = nullptr) SPECS {                       \
    return rpc_client->CallClientStreamingMethod<METHOD##Request, METHOD##Reply>(       \
        static_cast<                                                                    \
            AsyncClientStreamingMethod<SERVICE, METHOD##Request, METHOD##Reply>>(       \
            &SERVICE::Stub::async::METHOD),                                             \
        callback, ctx, #SERVICE ".grpc_client." #METHOD);                               \
  }

///
/// The callback-based gRPC client.
///

template <class GrpcService>
class GrpcCallbackClient {
 public:
  GrpcCallbackClient(const std::string &address, const int port,
                     instrumented_io_context &io_context, int num_threads = 1)
      : io_context_(io_context) {
    grpc::ChannelArguments argument;
    if (num_threads > 1) {
      grpc::ResourceQuota quota;
      quota.SetMaxThreads(num_threads);
      argument.SetResourceQuota(quota);
    }
    // Disable http proxy since it disrupts local connections. TODO(ekl) we should make
    // this configurable, or selectively set it for known local connections only.
    argument.SetInt(GRPC_ARG_ENABLE_HTTP_PROXY, 0);
    argument.SetMaxSendMessageSize(::RayConfig::instance().max_grpc_message_size());
    argument.SetMaxReceiveMessageSize(::RayConfig::instance().max_grpc_message_size());
    std::shared_ptr<grpc::Channel> channel =
        grpc::CreateCustomChannel(address + ":" + std::to_string(port),
                                  grpc::InsecureChannelCredentials(), argument);
    stub_ = GrpcService::NewStub(channel);
    shutdown_ = std::make_shared<std::atomic<bool>>(false);
  }

  ~GrpcCallbackClient() { shutdown_->store(true); }

  /// Call a unary RPC method.
  ///
  /// \tparam Request Type of the request message.
  /// \tparam Reply Type of the reply message.
  ///
  /// \param[in] method Pointer to the gRPC-generated `FooService::Stub::async::METHOD`
  /// function.
  /// \param[in] request The request message.
  /// \param[in] callback The callback function that handles the reply.
  /// \param[in] ctx The client context for this call. If not given, a
  /// default-constructed context will be used.
  /// \param[in] call_name A human-readable name for this call that will be used for
  /// event loop statistics collection.
  template <class Request, class Reply>
  void CallUnaryMethod(const AsyncUnaryMethod<GrpcService, Request, Reply> method,
                       const Request &request, const ClientCallback<Reply> &callback,
                       std::shared_ptr<grpc::ClientContext> ctx = nullptr,
                       const std::string call_name = "UNKNOWN_RPC") {
    // We use shared pointers for the client context and response in order to let the
    // below closure captures manage when this data is freed. This assumes that the
    // client context and the response will no longer be used by the gRPC library after
    // the gRPC-level callback is called (currently true) and is why we can provide gRPC
    // raw pointers without breaking the shared pointers' reference counting.
    if (ctx == nullptr) {
      ctx = std::make_shared<grpc::ClientContext>();
    }
    auto response = std::make_shared<Reply>();
    auto stats_handle = io_context_.stats().RecordStart(call_name);
    (stub_->async()->*method)(
        ctx.get(), &request, response.get(),
        // NOTE: The below callback will be called in the gRPC nexting thread.
        //
        // We directly capture the io_context_ by reference instead of just
        // capturing `this` since the gRPC-level callback may outlive this
        // GrpcCallbackClient instance, while we're more confident that it will not
        // outlive the event loop, which should only be destroyed on process exit.
        //
        // We capture the shutdown_ atomic flag shared pointer by copy so we can check
        // it after this GrpcCallbackClient has been destroyed; this serves as a proxy
        // for an io_context_ lifetime check.
        //
        // We capture the client context to ensure that it isn't deallocated
        // until the gRPC-level callback is invoked, which is required.
        //
        // TODO(Clark): Make this lifetime management nicer.
        [&io_context = io_context_, response, callback = std::move(callback), ctx,
         shutdown = shutdown_, stats_handle = std::move(stats_handle)](auto status) {
          // Tell the compiler that we need ctx in this closure even though it
          // isn't referenced (this prevents an unused lambda capture warning).
          static_cast<void>(ctx);
          if (!shutdown->load() && !io_context.stopped()) {
            io_context.post([response, callback = std::move(callback),
                             status = GrpcStatusToRayStatus(
                                 status)]() { callback(status, *response); },
                            std::move(stats_handle));
          }
        });
  }

  /// Call a client-streaming RPC method.
  ///
  /// \tparam Request Type of the request message.
  /// \tparam Reply Type of the reply message.
  ///
  /// \param[in] method Pointer to the gRPC-generated `FooService::Stub::async::METHOD`
  /// function.
  /// \param[in] callback The callback function that handles the reply.
  /// \param[in] call_name A human-readable name for this call that will be used for
  /// event loop statistics collection.
  template <class Request, class Reply>
  ClientStreamingWriter<GrpcService, Request, Reply> CallClientStreamingMethod(
      const AsyncClientStreamingMethod<GrpcService, Request, Reply> method,
      const ClientCallback<Reply> &callback,
      std::shared_ptr<grpc::ClientContext> ctx = nullptr,
      const std::string call_name = "UNKNOWN_RPC") {
    auto reactor = new ClientWriterReactor<GrpcService, Request, Reply>(
        callback, call_name, io_context_);
    // We use shared pointers for the client context and response in order to let the
    // below closure captures manage when this data is freed. This assumes that the
    // client context and the response will no longer be used by the gRPC library after
    // the gRPC-level callback is called (currently true) and is why we can provide gRPC
    // raw pointers without breaking the shared pointers' reference counting.
    if (ctx == nullptr) {
      ctx = std::make_shared<grpc::ClientContext>();
    }
    (stub_->async()->*method)(ctx.get(), &reactor->reseponse_, reactor);
    return ClientStreamingWriter<GrpcService, Request, Reply>(reactor);
  }

  std::unique_ptr<typename GrpcService::Stub> &GetStub() { return stub_; }

 private:
  /// The event loop to which client handlers will be posted.
  instrumented_io_context &io_context_;
  /// The gRPC-generated stub.
  std::unique_ptr<typename GrpcService::Stub> stub_;
  /// Whether the client has been shutdown. This is used by the gRPC-level callback to
  /// ensure that we don't try to reference any freed by-reference members, such as the
  /// asio io_context.
  std::shared_ptr<std::atomic<bool>> shutdown_;
};

}  // namespace rpc
}  // namespace ray
