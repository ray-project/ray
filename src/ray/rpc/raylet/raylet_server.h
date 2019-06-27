#ifndef RAY_RPC_OBJECT_MANAGER_SERVER_H
#define RAY_RPC_OBJECT_MANAGER_SERVER_H

#include "src/ray/rpc/grpc_server.h"
#include "src/ray/rpc/server_call.h"

#include "src/ray/protobuf/object_manager.grpc.pb.h"
#include "src/ray/protobuf/object_manager.pb.h"

namespace ray {
namespace rpc {

/// Implementations of the `RayletService`, check interface in
/// `src/ray/protobuf/object_manager.proto`.
class RayletServiceHandler {
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

/// The `GrpcService` for `RayletService`.
class RayletService : public GrpcService {
 public:
  /// Construct a `RayletService`.
  ///
  /// \param[in] port See `GrpcServer`.
  /// \param[in] handler The service handler that actually handle the requests.
  RayletService(const uint32_t port, boost::asio::io_service &main_service,
                      RayletServiceHandler &service_handler)
      : GrpcServer("Raylet", port, main_service),
        service_handler_(service_handler){};

  void RegisterServices(::grpc::ServerBuilder &builder) override {
    /// Register `RayletService`.
    builder.RegisterService(&service_);
  }

  void InitServerCallFactories(
      std::vector<std::pair<std::unique_ptr<ServerCallFactory>, int>>
          *server_call_factories_and_concurrencies) override {
    // Initialize the factory for `Push` requests.
    std::unique_ptr<ServerCallFactory> push_call_factory(
        new ServerCallFactoryImpl<RayletService, RayletServiceHandler,
                                  PushRequest, PushReply>(
            service_, &RayletService::AsyncService::RequestPush, service_handler_,
            &RayletServiceHandler::HandlePushRequest, cq_));
    server_call_factories_and_concurrencies->emplace_back(std::move(push_call_factory),
                                                          30);

    // Initialize the factory for `Pull` requests.
    std::unique_ptr<ServerCallFactory> pull_call_factory(
        new ServerCallFactoryImpl<RayletService, RayletServiceHandler,
                                  PullRequest, PullReply>(
            service_, &RayletService::AsyncService::RequestPull, service_handler_,
            &RayletServiceHandler::HandlePullRequest, cq_));
    server_call_factories_and_concurrencies->emplace_back(std::move(pull_call_factory),
                                                          2);

    // Initialize the factory for `FreeObjects` requests.
    std::unique_ptr<ServerCallFactory> free_objects_call_factory(
        new ServerCallFactoryImpl<RayletService, RayletServiceHandler,
                                  FreeObjectsRequest, FreeObjectsReply>(
            service_, &RayletService::AsyncService::RequestFreeObjects,
            service_handler_, &RayletServiceHandler::HandleFreeObjectsRequest,
            cq_));
    server_call_factories_and_concurrencies->emplace_back(
        std::move(free_objects_call_factory), 1);
  }

 private:
  /// The grpc async service object.
  RayletService::AsyncService service_;
  /// The service handler that actually handle the requests.
  RayletServiceHandler &service_handler_;
};

}  // namespace rpc
}  // namespace ray

#endif
