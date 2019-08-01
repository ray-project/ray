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
  CONNECT,
  WRITING,
  PENDING,
  STREAM_WRITES_DONE,
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
  virtual bool WriteStream(const ::google::protobuf::Message &request) = 0;

  virtual void WritesDone() = 0;

  virtual void AsyncReadNextReply() = 0;

  virtual ClientCallTag *GetReplyReaderTag() {}

  /// Common implementations for gRPC client call.
 public:
  ClientCall(ClientCallType type) : type_(type), state_(ClientCallState::CONNECT) {}

  const ClientCallType &GetType() { return type_; }

  void SetState(ClientCallState state) { state_ = state; }

  const ClientCallState &GetState() { return state_; }

  void TryCancel() { context_.TryCancel(); }

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

  // Stream call interfaces, not used in default call.
  bool WriteStream(const ::google::protobuf::Message &request) {}
  void WritesDone() {}
  void AsyncReadNextReply() {}

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
    GrpcService::Stub::*const)(grpc::ClientContext *, grpc::CompletionQueue *, void *);

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
      : ClientCall(ClientCallType::STREAM_ASYNC_CALL), callback_(callback) {}

  void Connect(typename GrpcService::Stub &stub,
               const AsyncRpcFunction<GrpcService, Request, Reply> async_rpc_function,
               grpc::CompletionQueue &cq) {
    client_stream_ =
        (stub.*async_rpc_function)(&context_, &cq, reinterpret_cast<void *>(tag_));
  }

  void WritesDone() override {
    // Server will receive a done tag after client cancels.
    TryCancel();
    // delete reader_tag_;
    // delete tag_;
    // state_ = ClientCallState::STREAM_WRITES_DONE;
    // client_stream_->WritesDone(reinterpret_cast<void *>(tag_));
  }

  bool WriteStream(const ::google::protobuf::Message &from) override {
    // The state of this call will be changed to PENDING in polling thread.
    if (state_ == ClientCallState::WRITING) {
      RAY_LOG(INFO) << "Stream is still in writing state.";
      return false;
    }
    // Construct the request with the specific type.
    Request request;
    request.CopyFrom(from);
    state_ = ClientCallState::WRITING;
    /// Only one write may be outstanding at any given time. This means that
    /// after calling `Write`, one must wait to receive a tag from the completion
    /// queue before calling `Write` again.
    client_stream_->Write(request, reinterpret_cast<void *>(tag_));
    return true;
  }

  Status GetStatus() override { return GrpcStatusToRayStatus(status_); }

  void AsyncReadNextReply() {
    client_stream_->Read(&reply_, reinterpret_cast<void *>(reader_tag_));
  }

  void SetClientCallTag(ClientCallTag *tag) { tag_ = tag; }

  void SetReplyReaderTag(ClientCallTag *reader_tag) { reader_tag_ = reader_tag; }

  ClientCallTag *GetReplyReaderTag() override { return reader_tag_; }

  ClientCallTag *GetClientCallTag() { return tag_; }

  void OnReplyReceived() override {
    if (callback_) {
      callback_(Status::OK(), reply_);
    }
    AsyncReadNextReply();
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
  /// Reading and writing in client stream call are asynchronous, we should use different
  /// type tag to distinguish the call tag caught in completion queue. We should put the
  /// reader tag into the completion queue when try to read a reply asynchronously in a
  /// stream,
  ClientCallTag *reader_tag_;

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
  enum class TagType {
    DEFAULT,
    STREAM_READER,
  };
  /// Constructor.
  ///
  /// \param call A `ClientCall` that represents a request.
  /// \param is_reader_tag Indicates whether it's a tag for reading reply from server.
  explicit ClientCallTag(std::shared_ptr<ClientCall> call,
                         TagType tag_type = TagType::DEFAULT)
      : call_(std::move(call)), tag_type_(tag_type) {}

  /// Get the wrapped `ClientCall`.
  const std::shared_ptr<ClientCall> &GetCall() const { return call_; }

  bool IsReaderTag() { return tag_type_ == TagType::STREAM_READER; }

 private:
  /// Pointer to the client call.
  std::shared_ptr<ClientCall> call_;
  /// The type of this tag.
  const TagType tag_type_;
};

}  // namespace rpc
}  // namespace ray

#endif
