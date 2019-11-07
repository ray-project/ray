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

/// Represents the client callback function of a particular rpc method.
///
/// \tparam Reply Type of the reply message.
template <class Reply>
using ClientCallback = std::function<void(const Status &status, const Reply &reply)>;

/// Implementation of the `ClientCall`. It represents a `ClientCall` for a particular
/// RPC method.
///
/// \tparam Reply Type of the Reply message.
template <class Reply>
class ClientCallImpl : public ClientCall {
 public:
  /// Constructor.
  ///
  /// \param[in] callback The callback function to handle the reply.
  explicit ClientCallImpl(const ClientCallback<Reply> &callback) : callback_(callback) {}

  Status GetStatus() override { return GrpcStatusToRayStatus(status_); }

  void OnReplyReceived() override {
    if (callback_ != nullptr) {
      callback_(GrpcStatusToRayStatus(status_), reply_);
    }
  }

 private:
  /// The reply message.
  Reply reply_;

  /// The callback function to handle the reply.
  ClientCallback<Reply> callback_;

  /// The response reader.
  std::unique_ptr<grpc_impl::ClientAsyncResponseReader<Reply>> response_reader_;

  /// gRPC status of this request.
  grpc::Status status_;

  /// Context for the client. It could be used to convey extra information to
  /// the server and/or tweak certain RPC behaviors.
  grpc::ClientContext context_;

  friend class ClientCallManager;
};

/// This class wraps a `ClientCall`, and is used as the `tag` of gRPC's `CompletionQueue`.
///
/// The lifecycle of a `ClientCallTag` is as follows.
///
/// When a client submits a new gRPC request, a new `ClientCallTag` object will be created
/// by `ClientCallMangager::CreateCall`. Then the object will be used as the tag of
/// `CompletionQueue`.
///
/// When the reply is received, `ClientCallMangager` will get the address of this object
/// via `CompletionQueue`'s tag. And the manager should call
/// `GetCall()->OnReplyReceived()` and then delete this object.
class ClientCallTag {
 public:
  /// Constructor.
  ///
  /// \param call A `ClientCall` that represents a request.
  explicit ClientCallTag(std::shared_ptr<ClientCall> call) : call_(std::move(call)) {}

  /// Get the wrapped `ClientCall`.
  const std::shared_ptr<ClientCall> &GetCall() const { return call_; }

 private:
  std::shared_ptr<ClientCall> call_;
};

/// Represents the generic signature of a `FooService::Stub::PrepareAsyncBar`
/// function, where `Foo` is the service name and `Bar` is the rpc method name.
///
/// \tparam GrpcService Type of the gRPC-generated service class.
/// \tparam Request Type of the request message.
/// \tparam Reply Type of the reply message.
template <class GrpcService, class Request, class Reply>
using PrepareAsyncFunction =
    std::unique_ptr<grpc_impl::ClientAsyncResponseReader<Reply>> (GrpcService::Stub::*)(
        grpc::ClientContext *context, const Request &request, grpc::CompletionQueue *cq);

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

 private:
  /// This function runs in a background thread. It keeps polling events from the
  /// `CompletionQueue`, and dispatches the event to the callbacks via the `ClientCall`
  /// objects.
  void PollEventsFromCompletionQueue() {
    void *got_tag;
    bool ok = false;
    auto deadline = gpr_inf_future(GPR_CLOCK_REALTIME);
    // Keep reading events from the `CompletionQueue` until it's shutdown.
    // NOTE(edoakes): we use AsyncNext here because for some unknown reason,
    // synchronous cq_.Next blocks indefinitely in the case that the process
    // received a SIGTERM.
    while (true) {
      auto status = cq_.AsyncNext(&got_tag, &ok, deadline);
      if (status == grpc::CompletionQueue::SHUTDOWN) {
        break;
      }
      if (status != grpc::CompletionQueue::TIMEOUT) {
        auto tag = reinterpret_cast<ClientCallTag *>(got_tag);
        if (ok && !main_service_.stopped()) {
          // Post the callback to the main event loop.
          main_service_.post([tag]() {
            tag->GetCall()->OnReplyReceived();
            // The call is finished, and we can delete this tag now.
            delete tag;
          });
        } else {
          delete tag;
        }
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
