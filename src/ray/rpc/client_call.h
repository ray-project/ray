#ifndef RAY_RPC_CLIENT_CALL_H
#define RAY_RPC_CLIENT_CALL_H

#include <grpcpp/grpcpp.h>
#include <boost/asio.hpp>

#include "ray/common/grpc_util.h"
#include "ray/common/status.h"

namespace ray {
namespace rpc {

enum class ClientCallType {
  DEFAULT_ASYNC_CALL = 1,
  STREAM_ASYNC_CALL = 2,
};

enum class ClientCallState {
  CREATE = 1,
  CONNECT = 2,
  WRITE = 3,
  READ = 4,
  WRITES_DONE = 5,
  FINISH = 6,
};

class ClientCallTag;
/// Represents an outgoing gRPC request.
///
/// NOTE(hchen): Compared to `ClientCallImpl`, this abstract interface doesn't use
/// template. This allows the users (e.g., `ClientCallMangager`) not having to use
/// template as well.
class ClientCall {
  /// Interfaces for default async call.
 public:
  /// Callback which will be called once the client received the reply.
  virtual void OnReplyReceived() = 0;

  /// Return status.
  virtual ray::Status GetStatus() = 0;

  /// Interfaces only for stream async call.
 public:
  virtual void WriteStream(::google::protobuf::Message &request) = 0;

  virtual void WritesDone() = 0;

  virtual bool IsReadingStream() = 0;

  /// Common implementations for gRPC client call.
 public:
  ClientCall(ClientCallType type) : type_(type), state_(ClientCallState::CREATE) {}

  const ClientCallType &GetCallType() { return type_; }

  const ClientCallState &GetCallState() { return state_; }

  virtual ~ClientCall() = default;

 protected:
  // The type of this client call.
  const ClientCallType type_;
  // Current state of this client call.
  ClientCallState state_;
  /// Context for the client. It could be used to convey extra information to
  /// the server and/or tweak certain RPC behaviors.
  grpc::ClientContext context_;
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
  explicit ClientCallImpl(const ClientCallback<Reply> &callback)
      : ClientCall(ClientCallType::DEFAULT_ASYNC_CALL), callback_(callback) {}

  Status GetStatus() override { return GrpcStatusToRayStatus(status_); }

  /// The callback to be called by `ClientCallManager` when the reply of this request is
  /// received.
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
  std::unique_ptr<grpc::ClientAsyncResponseReader<Reply>> response_reader_;

  /// gRPC status of this request.
  grpc::Status status_;

  friend class ClientCallManager;
};

template <class GrpcService, class Request, class Reply>
using AsyncRpcFunction = std::unique_ptr<grpc::ClientAsyncReaderWriter<Request, Reply>> (
    GrpcService::Stub::*)(grpc::ClientContext *, grpc::CompletionQueue, void *);

/// Implementation of the `ClientStreamCall`. It represents a `ClientStreamCall` for a
/// asynchronous streaming client call.
///
/// \tparam Reply Type of the Reply message.
template <class GrpcService, class Request, class Reply>
class ClientStreamCallImpl : public ClientCall {
 public:
  /// Constructor.
  ///
  /// \param[in] callback The callback function to handle the reply.
  explicit ClientStreamCallImpl(const ClientCallback<Reply> &callback)
      : callback_(callback) {}

  void Connect(typename GrpcService::Stub &stub,
               const AsyncRpcFunction<GrpcService, Request, Reply> async_rpc_function,
               grpc::CompletionQueue &cq) {
    state_ = ClientCallState::CONNECT;
    client_stream_ =
        (stub.*async_rpc_function)(&context_, &cq, reinterpret_cast<void *>(tag_));
    /// Wait for an asynchronous reading.
    AsyncReadNextMessage();
  }

  void WritesDone() {
    state_ = ClientCallState::WRITES_DONE;
    client_stream_->WritesDone(reinterpret_cast<void *>(tag_));
  }

  void WriteStream(::google::protobuf::Message &request) {
    state_ = ClientCallState::WRITE;
    client_stream_->Write(request, reinterpret_cast<void *>(tag_));
  }

  Status GetStatus() override { return GrpcStatusToRayStatus(status_); }

  bool IsReadingStream() { return reply_.IsInitialized(); }

  void AsyncReadNextMessage() {
    reply_.Clear();
    client_stream_->Read(&reply_, reinterpret_cast<void *>(tag_));
  }

  void SetClientCallTag(ClientCallTag *tag) { tag_ = tag; }
  ClientCallTag *GetClientCallTag() { return tag_; }

  void OnReplyReceived() override {
    if (callback_) {
      callback_(Status::OK(), reply_);
    }
    AsyncReadNextMessage();
  }

 private:
  /// The reply message.
  Reply reply_;

  /// The callback function to handle the reply.
  ClientCallback<Reply> callback_;

  /// Async writer and reader.
  std::unique_ptr<grpc::ClientAsyncReaderWriter<Request, Reply>> client_stream_;

  /// gRPC status of this request.
  grpc::Status status_;

  ClientCallTag *tag_;

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

}  // namespace rpc
}  // namespace ray

#endif
