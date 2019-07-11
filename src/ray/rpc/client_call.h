#ifndef RAY_RPC_CLIENT_CALL_H
#define RAY_RPC_CLIENT_CALL_H

#include <grpcpp/grpcpp.h>
#include <boost/asio.hpp>

#include "ray/common/grpc_util.h"
#include "ray/common/status.h"

namespace ray {
namespace rpc {

/// Represents an outgoing gRPC request.
///
/// The lifecycle of a `ClientCall` is as follows.
///
/// When a client submits a new gRPC request, a new `ClientCall` object will be created
/// by `ClientCallMangager::CreateCall`. Then the object will be used as the tag of
/// `CompletionQueue`.
///
/// When the reply is received, `ClientCallMangager` will get the address of this object
/// via `CompletionQueue`'s tag. And the manager should call `OnReplyReceived` and then
/// delete this object.
///
/// NOTE(hchen): Compared to `ClientCallImpl`, this abstract interface doesn't use
/// template. This allows the users (e.g., `ClientCallMangager`) not having to use
/// template as well.
class ClientCall {
 public:
  /// The callback to be called by `ClientCallManager` when the reply of this request is
  /// received.
  virtual void OnReplyReceived() = 0;
  /// Return status.
  virtual ray::Status GetStatus() = 0;

  virtual ~ClientCall() = default;
};

class ClientCallManager;

/// Reprents the client callback function of a particular rpc method.
///
/// \tparam Reply Type of the reply message.
template <class Reply>
using ClientCallback = std::function<void(const Status &status, const Reply &reply)>;

/// Implementaion of the `ClientCall`. It represents a `ClientCall` for a particular
/// RPC method.
///
/// \tparam Reply Type of the Reply message.
template <class Reply>
class ClientCallImpl : public ClientCall {
 public:
  Status GetStatus() override { return GrpcStatusToRayStatus(status_); }
  void OnReplyReceived() override {
    if (callback_ != nullptr) {
      callback_(GrpcStatusToRayStatus(status_), reply_);
    }
  }

 private:
  /// Constructor.
  ///
  /// \param[in] callback The callback function to handle the reply.
  ClientCallImpl(const ClientCallback<Reply> &callback) : callback_(callback) {}

  /// The reply message.
  Reply reply_;

  /// The callback function to handle the reply.
  ClientCallback<Reply> callback_;

  /// The response reader.
  std::unique_ptr<grpc::ClientAsyncResponseReader<Reply>> response_reader_;

  /// gRPC status of this request.
  grpc::Status status_;

  /// Context for the client. It could be used to convey extra information to
  /// the server and/or tweak certain RPC behaviors.
  grpc::ClientContext context_;

  friend class ClientCallManager;
};

/// Peprents the generic signature of a `FooService::Stub::PrepareAsyncBar`
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
/// Mutiple clients can share one `ClientCallManager`.
class ClientCallManager {
 public:
  /// Constructor.
  ///
  /// \param[in] main_service The main event loop, to which the callback functions will be
  /// posted.
  ClientCallManager(boost::asio::io_service &main_service) : main_service_(main_service) {
    // Start the polling thread.
    std::thread polling_thread(&ClientCallManager::PollEventsFromCompletionQueue, this);
    polling_thread.detach();
  }

  ~ClientCallManager() { cq_.Shutdown(); }

  /// Create a new `ClientCall` and send request.
  ///
  /// \param[in] stub The gRPC-generated stub.
  /// \param[in] prepare_async_function Pointer to the gRPC-generated
  /// `FooService::Stub::PrepareAsyncBar` function.
  /// \param[in] request The request message.
  /// \param[in] callback The callback function that handles reply.
  ///
  /// \tparam GrpcService Type of the gRPC-generated service class.
  /// \tparam Request Type of the request message.
  /// \tparam Reply Type of the reply message.
  template <class GrpcService, class Request, class Reply>
  ClientCall *CreateCall(
      typename GrpcService::Stub &stub,
      const PrepareAsyncFunction<GrpcService, Request, Reply> prepare_async_function,
      const Request &request, const ClientCallback<Reply> &callback) {
    // Create a new `ClientCall` object. This object will eventuall be deleted in the
    // `ClientCallManager::PollEventsFromCompletionQueue` when reply is received.
    auto call = new ClientCallImpl<Reply>(callback);
    // Send request.
    call->response_reader_ =
        (stub.*prepare_async_function)(&call->context_, request, &cq_);
    call->response_reader_->StartCall();
    call->response_reader_->Finish(&call->reply_, &call->status_, (void *)call);
    return call;
  }

 private:
  /// This function runs in a background thread. It keeps polling events from the
  /// `CompletionQueue`, and dispaches the event to the callbacks via the `ClientCall`
  /// objects.
  void PollEventsFromCompletionQueue() {
    void *got_tag;
    bool ok = false;
    // Keep reading events from the `CompletionQueue` until it's shutdown.
    while (cq_.Next(&got_tag, &ok)) {
      auto *call = reinterpret_cast<ClientCall *>(got_tag);
      if (ok) {
        // Post the callback to the main event loop.
        main_service_.post([call]() {
          call->OnReplyReceived();
          // The call is finished, we can delete the `ClientCall` object now.
          delete call;
        });
      } else {
        delete call;
      }
    }
  }

  /// The main event loop, to which the callback functions will be posted.
  boost::asio::io_service &main_service_;

  /// The gRPC `CompletionQueue` object used to poll events.
  grpc::CompletionQueue cq_;
};

}  // namespace rpc
}  // namespace ray

#endif
