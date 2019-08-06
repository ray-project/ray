#ifndef RAY_RPC_SERVER_CALL_FACTORY_H
#define RAY_RPC_SERVER_CALL_FACTORY_H

#include <grpcpp/grpcpp.h>

#include "ray/common/grpc_util.h"
#include "ray/common/status.h"
#include "ray/rpc/server_call.h"

namespace ray {
namespace rpc {

/// The factory that creates a particular kind of `ServerCall` objects.
class ServerCallFactory {
 public:
  /// Create a new `ServerCall` and request gRPC runtime to start accepting the
  /// corresponding type of requests.
  ///
  /// \return Pointer to the `ServerCall` object.
  virtual void CreateCall() const = 0;

  virtual ~ServerCallFactory() = default;

  ServerCallFactory(boost::asio::io_service &io_service,
                    const std::unique_ptr<grpc::ServerCompletionQueue> &cq)
      : io_service_(io_service), cq_(cq) {}

 protected:
  /// The event loop.
  boost::asio::io_service &io_service_;
  /// The `CompletionQueue`.
  const std::unique_ptr<grpc::ServerCompletionQueue> &cq_;
};

/// Represents the generic signature of a `FooService::AsyncService::RequestBar()`
/// function, where `Foo` is the service name and `Bar` is the rpc method name.
/// \tparam GrpcService Type of the gRPC-generated service class.
/// \tparam Request Type of the request message.
/// \tparam Reply Type of the reply message.
template <class GrpcService, class Request, class Reply>
using RequestCallFunction = void (GrpcService::AsyncService::*)(
    grpc::ServerContext *, Request *, grpc::ServerAsyncResponseWriter<Reply> *,
    grpc::CompletionQueue *, grpc::ServerCompletionQueue *, void *);

/// Implementation of `ServerCallFactory`
///
/// \tparam GrpcService Type of the gRPC-generated service class.
/// \tparam ServiceHandler Type of the handler that handles the request.
/// \tparam Request Type of the request message.
/// \tparam Reply Type of the reply message.
template <class GrpcService, class ServiceHandler, class Request, class Reply>
class ServerCallFactoryImpl : public ServerCallFactory {
  using AsyncService = typename GrpcService::AsyncService;

 public:
  /// Constructor.
  ///
  /// \param[in] service The gRPC-generated `AsyncService`.
  /// \param[in] request_call_function Pointer to the `AsyncService::RequestMethod`
  //  function.
  /// \param[in] service_handler The service handler that handles the request.
  /// \param[in] handle_request_function Pointer to the service handler function.
  /// \param[in] cq The `CompletionQueue`.
  /// \param[in] io_service The event loop.
  ServerCallFactoryImpl(
      boost::asio::io_service &io_service,
      const std::unique_ptr<grpc::ServerCompletionQueue> &cq, AsyncService &service,
      RequestCallFunction<GrpcService, Request, Reply> request_call_function,
      ServiceHandler &service_handler,
      HandleRequestFunction<ServiceHandler, Request, Reply> handle_request_function)
      : ServerCallFactory(io_service, cq),
        service_(service),
        request_call_function_(request_call_function),
        service_handler_(service_handler),
        handle_request_function_(handle_request_function) {}

  void CreateCall() const override {
    // Create a new `ServerCall`. This object will eventually be deleted by
    // `GrpcServer::PollEventsFromCompletionQueue`.
    auto call = std::make_shared<ServerCallImpl<ServiceHandler, Request, Reply>>(
        *this, service_handler_, handle_request_function_, io_service_);
    auto request_reader_tag = new ServerCallTag(call);
    call->SetServerCallTag(request_reader_tag);
    /// Request gRPC runtime to starting accepting this kind of request, using the call as
    /// the tag.
    (service_.*request_call_function_)(&call->context_, &call->request_,
                                       &call->response_writer_, cq_.get(), cq_.get(),
                                       reinterpret_cast<void *>(request_reader_tag));
  }

 private:
  /// The gRPC-generated `AsyncService`.
  AsyncService &service_;

  /// Pointer to the `AsyncService::RequestMethod` function.
  RequestCallFunction<GrpcService, Request, Reply> request_call_function_;

  /// Pointer to the service handler function.
  HandleRequestFunction<ServiceHandler, Request, Reply> handle_request_function_;

  /// The service handler that handles the request.
  ServiceHandler &service_handler_;
};

/// Represents the generic signature of a `FooService::AsyncService::RequestBar()`
/// function, where `Foo` is the service name and `Bar` is the rpc method name.
/// \tparam GrpcService Type of the gRPC-generated service class.
/// \tparam Request Type of the request message.
/// \tparam Reply Type of the reply message.
template <class GrpcService, class Request, class Reply>
using RequestStreamCallFunction = void (GrpcService::AsyncService::*)(
    grpc::ServerContext *, grpc::ServerAsyncReaderWriter<Reply, Request> *,
    grpc::CompletionQueue *, grpc::ServerCompletionQueue *, void *);

/// Stream server call factory, it's a kind of implementation of `ServerCallFactory`.
///
/// \tparam GrpcService Type of the gRPC-generated service class.
/// \tparam ServiceHandler Type of the handler that handles the request.
/// \tparam Request Type of the request message.
/// \tparam Reply Type of the reply message.
template <class GrpcService, class ServiceHandler, class Request, class Reply>
class ServerStreamCallFactoryImpl : public ServerCallFactory {
  using AsyncService = typename GrpcService::AsyncService;

 public:
  /// Constructor.
  ///
  /// \param[in] service The gRPC-generated `AsyncService`.
  /// \param[in] request_call_function Pointer to the `AsyncService::RequestMethod`
  //  function.
  /// \param[in] service_handler The service handler that handles the request.
  /// \param[in] handle_request_function Pointer to the service handler function.
  /// \param[in] cq The `CompletionQueue`.
  /// \param[in] io_service The event loop.
  ServerStreamCallFactoryImpl(
      boost::asio::io_service &io_service,
      const std::unique_ptr<grpc::ServerCompletionQueue> &cq, AsyncService &service,
      RequestStreamCallFunction<GrpcService, Request, Reply> request_stream_call_function,
      ServiceHandler &service_handler,
      HandleStreamRequestFunction<ServiceHandler, Request, Reply>
          handle_stream_request_function)
      : ServerCallFactory(io_service, cq),
        service_(service),
        request_stream_call_function_(request_stream_call_function),
        service_handler_(service_handler),
        handle_stream_request_function_(handle_stream_request_function) {}

  void CreateCall() const override {
    auto call = std::make_shared<ServerStreamCallImpl<ServiceHandler, Request, Reply>>(
        *this, service_handler_, handle_stream_request_function_, io_service_);
    auto request_reader_tag = new ServerCallTag(call);

    call->SetServerCallTag(request_reader_tag);
    call->SetState(ServerCallState::CONNECT);
    (service_.*request_stream_call_function_)(
        &call->context_, call->server_stream_.get(), cq_.get(), cq_.get(),
        reinterpret_cast<void *>(request_reader_tag));
  }

 private:
  /// The gRPC-generated `AsyncService`.
  AsyncService &service_;

  /// The `AsyncService::RequestMethod` function.
  RequestStreamCallFunction<GrpcService, Request, Reply> request_stream_call_function_;

  /// Service handler function to handle this server call.
  HandleStreamRequestFunction<ServiceHandler, Request, Reply>
      handle_stream_request_function_;

  /// The service handler that handles the requests, which contains all handler functions
  /// for this service.
  ServiceHandler &service_handler_;
};

}  // namespace rpc
}  // namespace ray

#endif
