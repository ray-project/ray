#ifndef RAY_RPC_CLIENT_CALL_H
#define RAY_RPC_CLIENT_CALL_H

#include <grpcpp/grpcpp.h>
#include <atomic>
#include <boost/asio.hpp>
#include <queue>

#include "ray/common/grpc_util.h"
#include "ray/common/status.h"

namespace ray {
namespace rpc {

enum class ClientCallType {
  DEFAULT_ASYNC_CALL,
  STREAM_ASYNC_CALL,
};

enum class ClientCallState {
  CONNECT,
  WRITING,
  PENDING,
  WRITES_DONE,
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
  /// Try to write a request into the rpc stream. If stream is not ready
  /// to write, we will write it into a writer buffer first.
  ///
  /// \param request The request message.
  /// \return False if the stream is not ready and the buffer is full.
  virtual bool WriteStream(const ::google::protobuf::Message &request) {}

  /// Finish the stream call. Requests in writer buffer will be discarded.
  virtual void WritesDone() {}

  /// Fetch a request from writer buffer and put it into the stream. Mark the
  /// stream as ready if the writer buffer is full.
  virtual void AsyncWriteNextRequest() {}

  virtual void AsyncReadNextReply() {}

  virtual ClientCallTag *GetReplyReaderTag() {}

  virtual void DeleteReplyReaderTag() {}

  virtual void ConnectFinish() {}

  virtual bool IsRunning() {}

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
  explicit ClientStreamCallImpl(const ClientCallback<Reply> &callback,
                                int max_buffer_size = 100)
      : ClientCall(ClientCallType::STREAM_ASYNC_CALL),
        callback_(callback),
        ready_to_write_(false),
        max_buffer_size_(max_buffer_size),
        is_running_(false) {}

  ~ClientStreamCallImpl() { RAY_LOG(INFO) << "Client stream call destruct."; }
  // Connect to remote server.
  // This is a synchronous call, which won't return until we receive the tag from
  // completion queue.
  void Connect(typename GrpcService::Stub &stub,
               const AsyncRpcFunction<GrpcService, Request, Reply> async_rpc_function,
               grpc::CompletionQueue &cq) {
    client_stream_ =
        (stub.*async_rpc_function)(&context_, &cq, reinterpret_cast<void *>(tag_));
    std::future<void> f(connect_promise_.get_future());
    is_running_ = true;
    // Block here until the connection to server has setup.
    f.get();
  }

  void ConnectFinish() override { connect_promise_.set_value(); }

  bool IsRunning() { return is_running_; }

  bool WriteStream(const ::google::protobuf::Message &from) override {
    std::unique_lock<std::mutex> lock(stream_call_mutex_);
    if (ready_to_write_) {
      ready_to_write_ = false;
      lock.unlock();
      // Construct the request with the specific type.
      Request request;
      request.CopyFrom(from);
      /// Only one write may be outstanding at any given time. This means that
      /// after calling `Write`, one must wait to receive a tag from the completion
      /// queue before calling `Write` again.
      client_stream_->Write(request, reinterpret_cast<void *>(tag_));
    } else {
      if (buffer_.size() >= max_buffer_size_) {
        RAY_LOG(INFO) << "Buffer of writing stream is full.";
        return false;
      }
      std::unique_ptr<Request> request(new Request);
      request->CopyFrom(from);
      buffer_.emplace(std::move(request));
    }
    return true;
  }

  void AsyncWriteNextRequest() override {
    if (!is_running_) {
      return;
    }
    std::lock_guard<std::mutex> lock(stream_call_mutex_);
    if (buffer_.empty()) {
      ready_to_write_ = true;
    } else {
      ready_to_write_ = false;
      client_stream_->Write(*buffer_.front(), reinterpret_cast<void *>(tag_));
      RAY_LOG(INFO) << "Client write.";
      buffer_.pop();
    }
  }

  void AsyncReadNextReply() override {
    if (!is_running_) {
      RAY_LOG(INFO) << "Call is stopped, just delete reader tag.";
      DeleteReplyReaderTag();
      return;
    }
    client_stream_->Read(&reply_, reinterpret_cast<void *>(reader_tag_));
    RAY_LOG(INFO) << "Put tag into the q, is running: " << is_running_;
  }

  /// We need to make sure that the tag cann't be deleted more than once.
  void DeleteReplyReaderTag() {
    if (reader_tag_) {
      delete reader_tag_;
      reader_tag_ = nullptr;
    }
  }

  void WritesDone() override {
    {
      std::lock_guard<std::mutex> lock(stream_call_mutex_);
      ready_to_write_ = false;
      if (!buffer_.empty()) {
        RAY_LOG(WARNING) << "The stream is being closed. There still are "
                         << buffer_.size()
                         << " requests in buffer, which will be discarded.";
      }
      // Cleanup all the cached requests.
      while (!buffer_.empty()) {
        buffer_.pop();
      }
    }
    is_running_ = false;
    // Server will receive a done tag after client cancels.
    state_ = ClientCallState::WRITES_DONE;
    client_stream_->WritesDone(reinterpret_cast<void *>(tag_));
    RAY_LOG(INFO) << "Client stream call put writes done into q.";
  }

  void OnReplyReceived() override {
    if (callback_) {
      callback_(Status::OK(), reply_);
    }
    AsyncReadNextReply();
  }

  void SetClientCallTag(ClientCallTag *tag) { tag_ = tag; }

  void SetReplyReaderTag(ClientCallTag *reader_tag) { reader_tag_ = reader_tag; }

  ClientCallTag *GetReplyReaderTag() override { return reader_tag_; }

  ClientCallTag *GetClientCallTag() { return tag_; }

  Status GetStatus() override { return GrpcStatusToRayStatus(status_); }

 private:
  /// The reply message.
  Reply reply_;

  /// The callback function to handle the reply.
  ClientCallback<Reply> callback_;

  /// Async writer and reader.
  std::unique_ptr<grpc::ClientAsyncReaderWriter<Request, Reply>> client_stream_;

  ClientCallTag *tag_;
  /// Reading and writing in client stream call are asynchronous, we should use different
  /// type of tags to distinguish the call tag got from completion queue in polling
  /// thread. We should put the reader tag into the completion queue when try to read a
  /// reply asynchronously in the stream.
  ClientCallTag *reader_tag_;

  /// gRPC status of this request.
  grpc::Status status_;

  /// Max size of writing buffer, we cann't write next request until the buffer is not
  /// full.
  int max_buffer_size_;

  // Indicates whether it's ready to write next request into stream. It's available only
  // if we receive the writer tag from the completion queue.
  bool ready_to_write_;

  // Mutex to protect the common variable in stream call.
  // Variables `buffer_` and `is_running_` are used in separate thread.
  std::mutex stream_call_mutex_;

  std::queue<std::unique_ptr<Request>> buffer_;

  // Reading and writing are diabled unless the connection between client and server has
  // setup successfully. Use `promise` and `future` to make the `Connect` function a
  // synchronous call.
  std::promise<void> connect_promise_;

  std::atomic<bool> is_running_;

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
    REPLY_READER,
  };
  /// Constructor.
  ///
  /// \param call A `ClientCall` that represents a request.
  /// \param is_reader_tag Indicates whether it's a tag for reading reply from server.
  explicit ClientCallTag(std::shared_ptr<ClientCall> call,
                         TagType tag_type = TagType::DEFAULT)
      : call_(call), tag_type_(tag_type) {}

  /// Get the wrapped `ClientCall`.
  const std::shared_ptr<ClientCall> &GetCall() const { return call_; }

  bool IsReplyReaderTag() { return tag_type_ == TagType::REPLY_READER; }

 private:
  /// Pointer to the client call.
  std::shared_ptr<ClientCall> call_;
  /// The type of this tag.
  const TagType tag_type_;
};

}  // namespace rpc
}  // namespace ray

#endif
