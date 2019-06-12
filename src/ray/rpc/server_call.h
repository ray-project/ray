#ifndef RAY_RPC_SERVER_CALL_H
#define RAY_RPC_SERVER_CALL_H

#include <grpcpp/grpcpp.h>

#include "ray/common/status.h"
#include "ray/rpc/util.h"

namespace ray {

using RequestDoneCallback = std::function<void(Status)>;

/// Represents state of a `ServerCall`.
enum class ServerCallState { PENDING, PROCECCSSING, SENDING_REPLY };

class ServerCallFactory;

/// Reprensents a incoming request of a gRPC server.
class ServerCall {
 public:
  /// Get the state of this `ServerCall`.
  virtual ServerCallState GetState() const = 0;

  /// Callback function to be called by `GrpcServer` when the request is received.
  virtual void OnRequestReceived() = 0;

  /// Get the factory that created this `ServerCall`.
  virtual const ServerCallFactory &GetFactory() const = 0;
};

/// A factory object that can create a particular kind of `ServerCall` objects.
class ServerCallFactory {
 public:
  /// Create a new `ServerCall` and request gRPC to start accepting the corresonding
  /// type of requests.
  virtual ServerCall *CreateCall() const = 0;
};

/// Implementation of `ServerCall`. It represents `ServerCall` for a particular
/// gRPC service method.
///
/// \tparam ServiceHandler Type of the handler that handles the request.
/// \tparam Request Type of the request message.
/// \tparam Reply Type of the reply message.
template <class ServiceHandler, class Request, class Reply>
class ServerCallImpl : public ServerCall {
  // Represents the generic signature of a `FooServiceHanler::HandleBar()`
  // method, where `Foo` is the name of the service and `Bar` is the name of the method.
  using HandleRequestFunction = void (ServiceHandler::*)(const Request &, Reply *,
                                                         RequestDoneCallback);

 public:
  /// Constructor.
  ///
  /// \param[in] factory The factory which created this call.
  /// \param[in] service_handler The service handler that handles the request.
  /// \param[in] handle_request_function Pointer to the service handler function.
  ServerCallImpl(const ServerCallFactory &factory, ServiceHandler &service_handler,
                 HandleRequestFunction handle_request_function)
      : state_(ServerCallState::PENDING),
        factory_(factory),
        service_handler_(service_handler),
        handle_request_function_(handle_request_function),
        response_writer_(&context_) {}

  ServerCallState GetState() const override { return state_; }

  void OnRequestReceived() override {
    state_ = ServerCallState::PROCECCSSING;
    (service_handler_.*handle_request_function_)(request_, &reply_,
                                                 [this](Status status) {
                                                   // When the handler is done with the
                                                   // request, tell gRPC to finish this
                                                   // request.
                                                   SendReply(status);
                                                 });
  }

  const ServerCallFactory &GetFactory() const override { return factory_; }

 private:
  /// Tell gRPC to finish this request.
  void SendReply(Status status) {
    state_ = ServerCallState::SENDING_REPLY;
    response_writer_.Finish(reply_, RayStatusToGrpcStatus(status), this);
  }

  /// State of this call.
  ServerCallState state_;

  /// The factory which created this call.
  const ServerCallFactory &factory_;

  /// The service handler that handles the request.
  ServiceHandler &service_handler_;

  /// Pointer to the service handler function.
  HandleRequestFunction handle_request_function_;

  /// Context for the request, allowing to tweak aspects of it such as the use
  /// of compression, authentication, as well as to send metadata back to the client.
  ::grpc::ServerContext context_;

  /// The reponse writer.
  ::grpc::ServerAsyncResponseWriter<Reply> response_writer_;

  /// The request message.
  Request request_;

  /// The reply message.
  Reply reply_;

  template <class T1, class T2, class T3, class T4>
  friend class ServerCallFactoryImpl;
};

template <class GcsService, class ServiceHandler, class Request, class Reply>
class ServerCallFactoryImpl : public ServerCallFactory {
  using AsyncService = typename GcsService::AsyncService;

  // Represents the generic signature of a `FooService::AsyncService::RequestBar()`
  // method, where `Foo` is the name of the service and `Bar` is the name of the method.
  using RequestCallFunction = void (AsyncService::*)(
      ::grpc::ServerContext *, Request *, ::grpc::ServerAsyncResponseWriter<Reply> *,
      ::grpc::CompletionQueue *, ::grpc::ServerCompletionQueue *, void *);

  // Represents the generic signature of a `FooServiceHanler::HandleBar()`
  // method, where `Foo` is the name of the service and `Bar` is the name of the method.
  using HandleRequestFunction = void (ServiceHandler::*)(const Request &, Reply *,
                                                         RequestDoneCallback);

 public:
  /// Constructor.
  ///
  /// \param[in] service The gRPC-generate `AsyncService`.
  /// \param[in] request_call_function Pointer to the `AsyncService::RequestMethod`
  //  fucntion.
  /// \param[in] service_handler The service handler that handles the request.
  /// \param[in] handle_request_function Pointer to the service handler function.
  /// \param[in] cq The `CompletionQueue`.
  ServerCallFactoryImpl(AsyncService &service, RequestCallFunction request_call_function,
                        ServiceHandler &service_handler,
                        HandleRequestFunction handle_request_function,
                        const std::unique_ptr<::grpc::ServerCompletionQueue> &cq)
      : service_(service),
        request_call_function_(request_call_function),
        service_handler_(service_handler),
        handle_request_function_(handle_request_function),
        cq_(cq) {}

  ServerCall *CreateCall() const override {
    // Create a new `ServerCall`.
    auto call = new ServerCallImpl<ServiceHandler, Request, Reply>(
        *this, service_handler_, handle_request_function_);
    /// Call `FooService::AsyncService::RequestBar()` function and use the call as the
    /// tag.
    (service_.*request_call_function_)(&call->context_, &call->request_,
                                       &call->response_writer_, cq_.get(), cq_.get(),
                                       call);
    return call;
  }

 private:
  /// The gRPC-generate `AsyncService`.
  AsyncService &service_;

  /// Pointer to the `AsyncService::RequestMethod` fucntion.
  RequestCallFunction request_call_function_;

  /// The service handler that handles the request.
  ServiceHandler &service_handler_;

  /// Pointer to the service handler function.
  HandleRequestFunction handle_request_function_;

  /// The `CompletionQueue`.
  const std::unique_ptr<::grpc::ServerCompletionQueue> &cq_;
};

}  // namespace ray

#endif
