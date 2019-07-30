#ifndef RAY_RPC_NODE_MANAGER_SERVER_H
#define RAY_RPC_NODE_MANAGER_SERVER_H

#include "ray/rpc/grpc_server.h"
#include "ray/rpc/server_call.h"

#include "src/ray/protobuf/test.grpc.pb.h"
#include "src/ray/protobuf/test.pb.h"

namespace ray {
namespace rpc {

/// Interface of the `TestServiceHandler`, see `src/ray/protobuf/test.proto`.
class TestServiceHandler {
 public:
  /// Handle a `DebugEcho` request.
  virtual void HandleDebugEcho(const DebugEchoRequest &request, DebugEchoReply *reply,
                               SendReplyCallback send_reply_callback) = 0;
  /// Handle `DebugStreamEcho` requests.
  virtual void HandleDebugStreamEcho(
      const DebugEchoRequest &request,
      StreamReplyWriter<DebugEchoRequest, DebugEchoReply> &stream_reply_writer) = 0;
};

/// The `GrpcService` for `TestService`.
class TestService : public GrpcService {
 public:
  /// Constructor.
  ///
  /// \param[in] io_service See super class.
  /// \param[in] handler The service handler that actually handle the requests.
  TestService(boost::asio::io_service &io_service, TestServiceHandler &service_handler)
      : GrpcService(io_service), service_handler_(service_handler){};

 protected:
  grpc::Service &GetGrpcService() override { return service_; }

  void InitServerCallFactories(
      const std::unique_ptr<grpc::ServerCompletionQueue> &cq,
      std::vector<std::pair<std::unique_ptr<ServerCallFactory>, int>>
          *server_call_factories_and_concurrencies,
      std::vector<std::unique_ptr<ServerCallFactory>> *server_stream_call_factories)
      override {
    // Initialize the factory for `DebugEcho` requests.
    std::unique_ptr<ServerCallFactory> debug_echo_call_factory(
        new ServerCallFactoryImpl<DebugEchoService, TestServiceHandler, DebugEchoRequest,
                                  DebugEchoReply>(
            main_service_, cq, service_,
            &DebugEchoService::AsyncService::RequestDebugEcho, service_handler_,
            &TestServiceHandler::HandleDebugEcho));
    // Set `DebugEcho`'s accept concurrency.
    server_call_factories_and_concurrencies->emplace_back(
        std::move(debug_echo_call_factory), 10);

    // Initialize the factory for `DebugStreamEcho` requests.
    std::unique_ptr<ServerCallFactory> debug_stream_echo_call_factory(
        new ServerStreamCallFactoryImpl<DebugEchoService, TestServiceHandler,
                                        DebugEchoRequest, DebugEchoReply>(
            main_service_, cq, service_,
            &DebugEchoService::AsyncService::RequestDebugStreamEcho, service_handler_,
            &TestServiceHandler::HandleDebugStreamEcho));
    // Set `DebugStreamEcho`'s accept concurrency.
    server_stream_call_factories->emplace_back(std::move(debug_stream_echo_call_factory));
  }

 private:
  /// The grpc async service object.
  DebugEchoService::AsyncService service_;

  /// The service handler that actually handle the requests.
  TestServiceHandler &service_handler_;
};

}  // namespace rpc
}  // namespace ray

#endif
