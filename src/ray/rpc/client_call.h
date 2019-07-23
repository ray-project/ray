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
  std::unique_ptr<grpc::ClientAsyncResponseReader<Reply>> response_reader_;

  /// gRPC status of this request.
  grpc::Status status_;

  /// Context for the client. It could be used to convey extra information to
  /// the server and/or tweak certain RPC behaviors.
  grpc::ClientContext context_;

  friend class ClientCallManager;
};

/// Represents an outgoing streaming gRPC call.
///
/// NOTE(hchen): Compared to `ClientCallImpl`, this abstract interface doesn't use
/// template. This allows the users (e.g., `ClientCallMangager`) not having to use
/// template as well.
template <class GrpcService, class Request, class Reply>
class ClientStreamCall {
 public:
  virtual void WriteStream(Request &request) = 0;
  virtual void ReadStream(Reply *reply) = 0;
  virtual void Connect() = 0;
  virtual void Close() = 0;
  virtual ~ClientStreamCall() = default;
};

template<class GrpcService, class Request, class Reply>
using RpcFunction = std::unique_ptr<grpc::Client

/// Implementation of the `ClientStreamCall`. It represents a `ClientStreamCall` for a asynchronous streaming client call.
///
/// \tparam Reply Type of the Reply message.
template <class GrpcService, class Request, class Reply>
class ClientStreamCallImpl : public ClientStreamCall<GrpcService, Request, Reply>,
                             public ClientCall {
  enum class Type {
    READ = 1,
    WRITE = 2,
    CONNECT = 3,
    WRITES_DONE = 4,
    FINISH = 5
  };
 public:
  /// Constructor.
  ///
  /// \param[in] callback The callback function to handle the reply.
  explicit ClientStreamCallImpl(const ClientCallback<Reply> &callback) : callback_(callback) {}

  void Connect(typename GrpcService::Stub &stub,
               const RpcFunction<GrpcService, Request, Reply> rpc_function,
               grpc::CompletionQueue &cq) {
    reader_writer_ = (stub.*rpc_function)(&context_, &cq, reinterpret_cast<void*>(Type::CONNECT));
  }

  void Close() {

  }

  void WriteStream() {

  }

  void ReadStream() {

  }

  Status GetStatus() override { return GrpcStatusToRayStatus(status_); }

  void OnReplyReceived() override {
    if (callback_ != nullptr) {
      callback_(GrpcStatusToRayStatus(status_), reply_);
    }
  }

 private:
  /// gRPC client context.
  grpc::ClientContext context_;

  /// The reply message.
  Reply reply_;

  /// The callback function to handle the reply.
  ClientCallback<Reply> callback_;

  /// Async writer and reader.
  std::unique_ptr<ClientAsyncReaderWriter<Request, Reply>> reader_writer_;

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

}  // namespace rpc
}  // namespace ray

#endif
