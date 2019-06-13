#ifndef RAY_RPC_CLIENT_CALL_H
#define RAY_RPC_CLIENT_CALL_H

#include <grpcpp/grpcpp.h>
#include <boost/asio.hpp>

#include "ray/common/status.h"
#include "ray/rpc/util.h"

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
class ClientCall {
 public:
  /// The callback to be called by `ClientCallManager` when the reply of this request is
  /// received.
  virtual void OnReplyReceived() = 0;
};

class ClientCallManager;

/// Implementaion of the `ClientCall`. It represents a `ClientCall` for a particular
/// RPC method.
///
/// \tparam Reply Type of the Reply message.
template <class Reply>
class ClientCallImpl : public ClientCall {
 public:
  /// Type of the callback function.
  using Callback = std::function<void(const Status &status, const Reply &reply)>;

  void OnReplyReceived() override { callback_(GrpcStatusToRayStatus(status_), reply_); }

 private:
  /// Constructor.
  ///
  /// \param[in] callback The callback function to handle the reply.
  ClientCallImpl(const Callback &callback) : callback_(callback) {}

  /// The reply message.
  Reply reply_;

  /// The callback function to handle the reply.
  Callback callback_;

  /// The response reader.
  std::unique_ptr<grpc::ClientAsyncResponseReader<Reply>> response_reader_;

  /// gRPC status of this request.
  ::grpc::Status status_;

  /// Context for the client. It could be used to convey extra information to
  /// the server and/or tweak certain RPC behaviors.
  ::grpc::ClientContext context_;

  friend class ClientCallManager;
};

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
    auto poll = [this]() {
      void *got_tag;
      bool ok = false;
      // Keep reading events from the `CompletionQueue`.
      while (cq_.Next(&got_tag, &ok)) {
        RAY_CHECK(ok);
        ClientCall *call = reinterpret_cast<ClientCall *>(got_tag);
        // Post the callback to the main event loop.
        main_service_.post([call]() {
          call->OnReplyReceived();
          // The call is finished, we can delete the `ClientCall` object now.
          delete call;
        });
      }
    };
    polling_thread_.reset(new std::thread(std::move(poll)));
  }

  ~ClientCallManager() { cq_.Shutdown(); }

  /// Create a new `ClientCall`.
  ///
  /// \param[in] stub The gRPC-generated stub.
  /// \param[in] request The request message.
  /// \param[in] callback The callback function that handles reply.
  template <class GrpcService, class Request, class Reply, class Callback>
  ClientCall *CreateCall(const std::unique_ptr<typename GrpcService::Stub> &stub,
                         const Request &request, const Callback &callback) {
    auto call = new ClientCallImpl<Reply>(callback);
    call->response_reader_ =
        stub->PrepareAsyncForwardTask(&call->context_, request, &cq_);
    call->response_reader_->StartCall();
    call->response_reader_->Finish(&call->reply_, &call->status_, (void *)call);
    return call;
  }

 private:
  /// The main event loop, to which the callback functions will be posted.
  boost::asio::io_service &main_service_;

  /// The polling thread.
  std::unique_ptr<std::thread> polling_thread_;

  /// The gRPC `CompletionQueue` object used to poll events.
  ::grpc::CompletionQueue cq_;
};

}  // namespace rpc
}  // namespace ray

#endif
