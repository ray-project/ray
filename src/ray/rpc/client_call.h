#ifndef RAY_RPC_CLIENT_CALL_H
#define RAY_RPC_CLIENT_CALL_H

#include <grpcpp/grpcpp.h>
#include <boost/asio.hpp>
#include <queue>

#include "ray/common/grpc_util.h"
#include "ray/common/status.h"

namespace ray {
namespace rpc {

/// The type of the client call.
enum class ClientCallType {
  UNARY_ASYNC_CALL,
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
/// It's the common parts of all types of client calls.
/// The interfaces that should be implemented by both types of client calls are defined as
/// pure virtual function. Otherwise we provide a default implementation to avoid unused
/// definitions in the subclass.
///
/// NOTE(hchen): Compared to `ClientCallImpl`, this abstract interface doesn't use
/// template. This allows the users (e.g., `ClientCallMangager`) not having to use
/// template as well.
class ClientCall {
 public:
  /// Interfaces for default async call or stream async call.

  /// Callback which will be called once the client received the reply.
  virtual void OnReplyReceived() = 0;

 public:
  /// Interfaces only for stream async call.

  /// Only one write may be outstanding at any given time. This means that
  /// after calling `Write`, one must wait to receive a tag from the completion
  /// queue before calling `Write` again.
  /// Try to write a request into the rpc stream. If stream is not ready
  /// to write, we will write it into a writer buffer first to avoid calling `Write`
  /// twice, which will cause grpc crash.
  ///
  /// \param request The request message.
  /// \return False if the stream is not ready and the buffer is full.
  virtual bool WriteStream(const ::google::protobuf::Message &request) {}

  /// Once we've finished writing our client's requests to the stream using `Write()`,
  /// we need to call `WritesDone()` on the stream to let gRPC know that we've
  /// finished writing.
  virtual void WritesDone() {}

  /// Fetch a request from writer buffer and put it into the stream. Mark the
  /// stream as ready if the writer buffer is empty.
  virtual void AsyncWriteNextRequest() {}

  /// Read next reply from the server.
  virtual void AsyncReadNextReply() {}

  /// Get the reply reader tag.
  virtual ClientCallTag *GetReplyReaderTag() {}

  virtual void DeleteReplyReaderTag() {}

  /// Handler when we have received the tag with `CONNECT` state from completion queue.
  virtual void OnConnectingFinished() {}

  /// Indicates whether the stream call is running.
  virtual bool IsRunning() {}

 public:
  /// Common implementations for gRPC client call, all subclasses share the same
  /// implementation.

  ClientCall(ClientCallType type) : type_(type), state_(ClientCallState::CONNECT) {}

  const ClientCallType &GetType() { return type_; }

  void SetState(ClientCallState state) { state_ = state; }

  const ClientCallState &GetState() { return state_; }

  Status GetStatus() { return GrpcStatusToRayStatus(status_); }

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
  /// gRPC status of this request.
  grpc::Status status_;
};

class ClientCallManager;

/// Represents the client callback function of a particular rpc method.
///
/// \tparam Reply Type of the reply message.
template <class Reply>
using ClientCallback = std::function<void(const Status &status, const Reply &reply)>;

/// Implementation of the `ClientCall`. It represents a `ClientCall` for a particular
/// RPC method.
/// When the reply is received, `ClientCallMangager` will get the address of the tag which
/// contains this object via `CompletionQueue`'s tag. And the manager should call
/// `GetCall()->OnReplyReceived()` and then delete the tag.
///
/// \tparam Reply Type of the Reply message.
template <class Reply>
class ClientCallImpl : public ClientCall {
 public:
  /// Constructor.
  ///
  /// \param[in] callback The callback function to handle the reply.
  explicit ClientCallImpl(const ClientCallback<Reply> &callback)
      : ClientCall(ClientCallType::UNARY_ASYNC_CALL), callback_(callback) {}

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
/// \tparam GrpcService Type of the grpc service.
/// \tparam Request Type of the Request message.
/// \tparam Reply Type of the Reply message.
template <class GrpcService, class Request, class Reply>
class ClientStreamCallImpl : public ClientCall {
 public:
  /// Construct a new client stream call object.
  ///
  /// \param callback The callback used to handle reply messages.
  /// \param max_buffer_size  The max buffer size for writing requests. Requests written
  ///               by user will be placed into the buffer if the completion queue is not
  ///               ready.
  explicit ClientStreamCallImpl(const ClientCallback<Reply> &callback,
                                int max_buffer_size = 1000)
      : ClientCall(ClientCallType::STREAM_ASYNC_CALL),
        callback_(callback),
        ready_to_write_(false),
        max_buffer_size_(max_buffer_size),
        is_running_(false) {}

  ~ClientStreamCallImpl() { RAY_LOG(DEBUG) << "Client stream call destructs."; }
  // Connect to remote server.
  // This is a synchronous call, which won't return until we receive the tag from
  // completion queue. We won't receive replies if we read before getting tag with a
  // `CONNECT` state.
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

  void OnConnectingFinished() override { connect_promise_.set_value(); }

  bool IsRunning() { return is_running_; }

  bool WriteStream(const ::google::protobuf::Message &from) override {
    // Construct the request with the specific type.
    std::unique_ptr<Request> request(new Request);
    request->CopyFrom(from);
    std::unique_lock<std::mutex> lock(stream_call_mutex_);
    if (ready_to_write_) {
      ready_to_write_ = false;
      lock.unlock();
      client_stream_->Write(*request, reinterpret_cast<void *>(tag_));
    } else {
      if (buffer_.size() >= max_buffer_size_) {
        RAY_LOG(INFO) << "Buffer of writing stream is full.";
        return false;
      }
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
      buffer_.pop();
    }
  }

  void AsyncReadNextReply() override {
    if (!is_running_) {
      RAY_LOG(DEBUG) << "The call is closed, deleting reader tag.";
      // Just delete the reply reader tag when reader tag hasn't been put into the
      // completion queue.
      DeleteReplyReaderTag();
      return;
    }
    client_stream_->Read(&reply_, reinterpret_cast<void *>(reader_tag_));
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
    state_ = ClientCallState::WRITES_DONE;
    client_stream_->WritesDone(reinterpret_cast<void *>(tag_));
  }

  void OnReplyReceived() override {
    if (callback_) {
      callback_(Status::OK(), reply_);
    }
    AsyncReadNextReply();
  }

  void SetClientCallTag(ClientCallTag *tag) { tag_ = tag; }

  ClientCallTag *GetClientCallTag() { return tag_; }

  void SetReplyReaderTag(ClientCallTag *reader_tag) { reader_tag_ = reader_tag; }

  ClientCallTag *GetReplyReaderTag() override { return reader_tag_; }

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

  /// Max size of writing buffer, we cann't write next request unless the buffer is not
  /// full or the completion queue is ready to write.
  int max_buffer_size_;

  // Indicates whether it's ready to write next request into stream. It's available only
  // if we receive the writer tag from the completion queue.
  bool ready_to_write_;

  // Mutex to protect the shared variables in stream call.
  // Variables `buffer_` and `ready_to_write_` are used in separate threads.
  std::mutex stream_call_mutex_;

  // Buffer for sending requests to the server. We need write the request into the queue
  // when the server is not ready to write.
  std::queue<std::unique_ptr<Request>> buffer_;

  // Reading and writing are diabled unless the connection between client and server has
  // setup successfully. Use `promise` and `future` to make the `Connect` function a
  // synchronous call.
  std::promise<void> connect_promise_;

  bool is_running_;

  friend class ClientCallManager;
};

/// This class wraps a `ClientCall`, and is used as the `tag` of gRPC's `CompletionQueue`.
///
/// The lifecycle of a `ClientCallTag` is as follows.
///
/// For a default asynchronous grpc call. When a client submits a new gRPC request, a new
/// `ClientCallTag` object will be created by `ClientCallMangager::CreateCall`. Then the
/// object will be used as the tag of `CompletionQueue`.
///
/// For a stream grpc call, we need to used two tags to distinguish the type of tags got
/// from the completion queue. When received a reply reader tag we should handle the reply
/// from the server. For a default tag, we should keep trace of the state of the client
/// stream client to connect, send requests and finish the stream.
class ClientCallTag {
 public:
  enum class TagType {
    /// A tag for common uses, such as connecting, sending requests and sending writes
    /// done.
    REQUEST,
    /// Only used to read reply from the server. Each client stream call should contains
    /// one
    /// reply reader tag to handle reply.
    REPLY,
  };
  /// Constructor.
  ///
  /// \param call A `ClientCall` that represents a request.
  /// \param is_reader_tag Indicates whether it's a tag for reading reply from server.
  explicit ClientCallTag(std::shared_ptr<ClientCall> call,
                         TagType tag_type = TagType::REQUEST)
      : call_(call), tag_type_(tag_type) {}

  /// Get the wrapped `ClientCall`.
  const std::shared_ptr<ClientCall> &GetCall() const { return call_; }

  /// Indicates whether this tag is a reply reader tag.
  bool IsReplyReaderTag() { return tag_type_ == TagType::REPLY; }

 private:
  /// Shared pointer to the client call, the call won't be freed until all tags pointing
  /// to it have be deleted.
  std::shared_ptr<ClientCall> call_;
  /// The type of this tag.
  const TagType tag_type_;
};

}  // namespace rpc
}  // namespace ray

#endif
