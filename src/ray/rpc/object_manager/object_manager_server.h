#ifndef RAY_RPC_OBJECT_MANAGER_SERVER_H
#define RAY_RPC_OBJECT_MANAGER_SERVER_H

#include "src/ray/rpc/grpc_server.h"
#include "src/ray/rpc/server_call.h"

#include "src/ray/protobuf/object_manager.grpc.pb.h"
#include "src/ray/protobuf/object_manager.pb.h"

namespace ray {
namespace rpc {

/// Implementations of the `ObjectManagerGrpcService`, check interface in
/// `src/ray/protobuf/object_manager.proto`.
class ObjectManagerServiceHandler {
 public:
  /// Handle a `Push` request.
  /// The implementation can handle this request asynchronously. When handling is done,
  /// the `send_reply_callback` should be called.
  ///
  /// \param[in] request The request message.
  /// \param[out] reply The reply message.
  /// \param[in] send_reply_callback The callback to be called when the request is done.
  virtual void HandlePushRequest(const PushRequest &request, PushReply *reply,
                                 SendReplyCallback send_reply_callback) = 0;
  /// Handle a `Pull` request
  virtual void HandlePullRequest(const PullRequest &request, PullReply *reply,
                                 SendReplyCallback send_reply_callback) = 0;
  /// Handle a `FreeObjects` request
  virtual void HandleFreeObjectsRequest(const FreeObjectsRequest &request,
                                        FreeObjectsReply *reply,
                                        SendReplyCallback send_reply_callback) = 0;
};

/// The `GrpcService` for `ObjectManagerGrpcService`.
class ObjectManagerGrpcService : public GrpcService {
 public:
  /// Construct a `ObjectManagerGrpcService`.
  ///
  /// \param[in] port See `GrpcService`.
  /// \param[in] handler The service handler that actually handle the requests.
  ObjectManagerGrpcService(boost::asio::io_service &io_service,
                           ObjectManagerServiceHandler &service_handler)
      : GrpcService(io_service), service_handler_(service_handler){};

 protected:
  grpc::Service &GetGrpcService() override { return service_; }

  void InitServerCallFactories(
      const std::unique_ptr<grpc::ServerCompletionQueue> &cq,
      std::vector<std::pair<std::unique_ptr<ServerCallFactory>, int>>
          *server_call_factories_and_concurrencies) override {
    // Initialize the factory for `Push` requests.
    std::unique_ptr<ServerCallFactory> push_call_factory(
        new ServerCallFactoryImpl<ObjectManagerService, ObjectManagerServiceHandler,
                                  PushRequest, PushReply>(
            service_, &ObjectManagerService::AsyncService::RequestPush, service_handler_,
            &ObjectManagerServiceHandler::HandlePushRequest, cq, main_service_));
    server_call_factories_and_concurrencies->emplace_back(std::move(push_call_factory),
                                                          50);

    // Initialize the factory for `Pull` requests.
    std::unique_ptr<ServerCallFactory> pull_call_factory(
        new ServerCallFactoryImpl<ObjectManagerService, ObjectManagerServiceHandler,
                                  PullRequest, PullReply>(
            service_, &ObjectManagerService::AsyncService::RequestPull, service_handler_,
            &ObjectManagerServiceHandler::HandlePullRequest, cq, main_service_));
    server_call_factories_and_concurrencies->emplace_back(std::move(pull_call_factory),
                                                          50);

    // Initialize the factory for `FreeObjects` requests.
    std::unique_ptr<ServerCallFactory> free_objects_call_factory(
        new ServerCallFactoryImpl<ObjectManagerService, ObjectManagerServiceHandler,
                                  FreeObjectsRequest, FreeObjectsReply>(
            service_, &ObjectManagerService::AsyncService::RequestFreeObjects,
            service_handler_, &ObjectManagerServiceHandler::HandleFreeObjectsRequest, cq,
            main_service_));
    server_call_factories_and_concurrencies->emplace_back(
        std::move(free_objects_call_factory), 2);
  }

 private:
  /// The grpc async service object.
  ObjectManagerService::AsyncService service_;
  /// The service handler that actually handle the requests.
  ObjectManagerServiceHandler &service_handler_;
};

}  // namespace rpc
}  // namespace ray

#endif
