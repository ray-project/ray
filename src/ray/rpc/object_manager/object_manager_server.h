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
  /// The implementation can handle this request asynchronously. When hanling is done, the
  /// `done_callback` should be called.
  ///
  /// \param[in] request The request message.
  /// \param[out] reply The reply message.
  /// \param[in] done_callback The callback to be called when the request is done.
  virtual void HandlePushRequest(const PushRequest &request, PushReply *reply,
                                 RequestDoneCallback done_callback) = 0;
  /// Handle a `Pull` request
  virtual void HandlePullRequest(const PullRequest &request, PullReply *reply,
                                 RequestDoneCallback done_callback) = 0;
  /// Handle a `FreeObjects` request
  virtual void HandleFreeObjectsRequest(const FreeObjectsRequest &request,
                                        FreeObjectsReply *reply,
                                        RequestDoneCallback done_callback) = 0;
};

/// The `GrpcService` for `ObjectManagerGrpcService`.
class ObjectManagerGrpcService : public GrpcService {
 public:
  /// Construct a `ObjectManagerGrpcService`.
  ///
  /// \param[in] port See `GrpcService`.
  /// \param[in] handler The service handler that actually handle the requests.
  ObjectManagerGrpcService(boost::asio::io_service &main_service,
                           ObjectManagerServiceHandler &service_handler)
      : GrpcService("ObjectManager", port, main_service),
        service_handler_(service_handler){};

  void RegisterServices(::grpc::ServerBuilder &builder) override {
    /// Register `ObjectManagerGrpcService`.
    builder.RegisterService(&service_);
  }

  void InitServerCallFactories(
      std::vector<std::pair<std::unique_ptr<ServerCallFactory>, int>>
          *server_call_factories_and_concurrencies) override {
    // Initialize the factory for `Push` requests.
    std::unique_ptr<ServerCallFactory> push_call_factory(
        new ServerCallFactoryImpl<ObjectManagerGrpcService,
                                  ObjectManagerServiceHandler, PushRequest,
                                  PushReply>(
            service_, &ObjectManagerGrpcService::AsyncService::RequestPush,
            service_handler_, &ObjectManagerServiceHandler::HandlePushRequest, cq_,
            main_service_));
    server_call_factories_and_concurrencies->emplace_back(std::move(push_call_factory),
                                                          60);

    // Initialize the factory for `Pull` requests.
    std::unique_ptr<ServerCallFactory> pull_call_factory(
        new ServerCallFactoryImpl<ObjectManagerGrpcService,
                                  ObjectManagerServiceHandler, PullRequest,
                                  PullReply>(
            service_, &ObjectManagerGrpcService::AsyncService::RequestPull,
            service_handler_, &ObjectManagerServiceHandler::HandlePullRequest, cq_,
            main_service_));
    server_call_factories_and_concurrencies->emplace_back(std::move(pull_call_factory),
                                                          60);

    // Initialize the factory for `FreeObjects` requests.
    std::unique_ptr<ServerCallFactory> free_objects_call_factory(
        new ServerCallFactoryImpl<ObjectManagerGrpcService,
                                  ObjectManagerServiceHandler, FreeObjectsRequest,
                                  FreeObjectsReply>(
            service_, &ObjectManagerGrpcService::AsyncService::RequestFreeObjects,
            service_handler_, &ObjectManagerServiceHandler::HandleFreeObjectsRequest,
            cq_, main_service_));
    server_call_factories_and_concurrencies->emplace_back(
        std::move(free_objects_call_factory), 2);
  }

 private:
  /// The grpc async service object.
  ObjectManagerGrpcService::AsyncService service_;
  /// The service handler that actually handle the requests.
  ObjectManagerServiceHandler &service_handler_;
};

}  // namespace rpc
}  // namespace ray

#endif
