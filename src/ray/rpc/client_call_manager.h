#ifndef RAY_RPC_CLIENT_CALL_MANAGER_H
#define RAY_RPC_CLIENT_CALL_MANAGER_H

#include <grpcpp/grpcpp.h>
#include <boost/asio.hpp>

#include "ray/common/grpc_util.h"
#include "ray/common/status.h"
#include "ray/rpc/client_call.h"

namespace ray {
namespace rpc {

/// Represents the generic signature of a `FooService::Stub::PrepareAsyncBar`
/// function, where `Foo` is the service name and `Bar` is the rpc method name.
///
/// \tparam GrpcService Type of the gRPC-generated service class.
/// \tparam Request Type of the request message.
/// \tparam Reply Type of the reply message.
template <class GrpcService, class Request, class Reply>
using PrepareAsyncFunction = std::unique_ptr<grpc::ClientAsyncResponseReader<Reply>> (
    GrpcService::Stub::*)(grpc::ClientContext *context, const Request &request,
                          grpc::CompletionQueue *cq);

/// `ClientCallManager` is used to manage outgoing gRPC requests and the lifecycles of
/// `ClientCall` objects.
///
/// It maintains a thread that keeps polling events from `CompletionQueue`, and post
/// the callback function to the main event loop when a reply is received.
///
/// Multiple clients can share one `ClientCallManager`.
class ClientCallManager {
 public:
  /// Constructor.
  ///
  /// \param[in] main_service The main event loop, to which the callback functions will be
  /// posted.
  explicit ClientCallManager(boost::asio::io_service &main_service)
      : main_service_(main_service) {
    // Start the polling thread.
    polling_thread_ =
        std::thread(&ClientCallManager::PollEventsFromCompletionQueue, this);
  }

  ~ClientCallManager() {
    cq_.Shutdown();
    polling_thread_.join();
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
  template <class GrpcService, class Request, class Reply>
  std::shared_ptr<ClientCall> CreateCall(
      typename GrpcService::Stub &stub,
      const PrepareAsyncFunction<GrpcService, Request, Reply> prepare_async_function,
      const Request &request, const ClientCallback<Reply> &callback) {
    auto call = std::make_shared<ClientCallImpl<Reply>>(callback);
    // Send request.
    call->response_reader_ =
        (stub.*prepare_async_function)(&call->context_, request, &cq_);
    call->response_reader_->StartCall();
    // Create a new tag object. This object will eventually be deleted in the
    // `ClientCallManager::PollEventsFromCompletionQueue` when reply is received.
    //
    // NOTE(chen): Unlike `ServerCall`, we can't directly use `ClientCall` as the tag.
    // Because this function must return a `shared_ptr` to make sure the returned
    // `ClientCall` is safe to use. But `response_reader_->Finish` only accepts a raw
    // pointer.
    auto tag = new ClientCallTag(call);
    call->response_reader_->Finish(&call->reply_, &call->status_, (void *)tag);
    return call;
  }

  template <class GrpcService, class Request, class Reply>
  std::shared_ptr<ClientCall> CreateStreamCall(
      typename GrpcService::Stub &stub,
      const AsyncRpcFunction<GrpcService, Request, Reply> async_rpc_function,
      const ClientCallback<Reply> &callback) {
    auto call =
        std::make_shared<ClientStreamCallImpl<GrpcService, Request, Reply>>(callback);
    auto tag = new ClientCallTag(call);
    auto reader_tag = new ClientCallTag(call, ClientCallTag::TagType::STREAM_READER);
    // Should set tag before `Connect` because the tag will be put into completion queue
    // in connect function.
    call->SetClientCallTag(tag);
    call->SetReplyReaderTag(reader_tag);
    // Setup connection with remote server.
    call->Connect(stub, async_rpc_function, cq_);
    return call;
  }

 private:
  /// This function runs in a background thread. It keeps polling events from the
  /// `CompletionQueue`, and dispatches the event to the callbacks via the `ClientCall`
  /// objects.
  void PollEventsFromCompletionQueue() {
    void *got_tag;
    bool ok = false;
    // Keep reading events from the `CompletionQueue` until it's shutdown.
    while (cq_.Next(&got_tag, &ok)) {
      auto tag = reinterpret_cast<ClientCallTag *>(got_tag);
      auto call = tag->GetCall();
      auto type = call->GetType();
      if (ok) {
        auto state = call->GetState();
        if (type == ClientCallType::DEFAULT_ASYNC_CALL) {
          // Post the callback to the main event loop.
          main_service_.post([tag]() {
            tag->GetCall()->OnReplyReceived();
            // The call is finished, and we can delete this tag now.
            delete tag;
          });
        } else if (type == ClientCallType::STREAM_ASYNC_CALL) {
          if (tag->IsReaderTag()) {
            main_service_.post([tag]() { tag->GetCall()->OnReplyReceived(); });
          } else {
            switch (state) {
            case ClientCallState::CONNECT:
              call->AsyncReadNextReply();
              break;
            case ClientCallState::WRITING:
              call->SetState(ClientCallState::PENDING);
              break;
            case ClientCallState::STREAM_WRITES_DONE:
              RAY_LOG(INFO) << "Client call writes done.";
              delete call->GetReplyReaderTag();
              delete tag;
              break;
            default:
              RAY_LOG(INFO) << "Shouldn't reach here.";
              break;
            }
          }
        }
      } else {
        RAY_LOG(INFO) << "Try to remove tag.";
        if (type == ClientCallType::STREAM_ASYNC_CALL) {
          // delete call->GetReplyReaderTag();
        }
        delete tag;
      }
    }
  }

  /// The main event loop, to which the callback functions will be posted.
  boost::asio::io_service &main_service_;

  /// The gRPC `CompletionQueue` object used to poll events.
  grpc::CompletionQueue cq_;

  /// Polling thread to check the completion queue.
  std::thread polling_thread_;
};

}  // namespace rpc
}  // namespace ray

#endif
