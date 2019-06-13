#ifndef RAY_RPC_OBJECT_MANAGER_SERVER_H
#define RAY_RPC_OBJECT_MANAGER_SERVER_H

#include "ray/rpc/grpc_server.h"
#include "ray/rpc/server_call.h"

#include "src/ray/protobuf/object_manager.grpc.pb.h"
#include "src/ray/protobuf/object_manager.pb.h"

namespace ray {

/// Implementations of the `ObjectManagerService`, check interface in `src/ray/protobuf/object_manager.proto`.
class ObjectManagerServiceHandlers {
 public:
  /// Handle a `Push` request.
  /// The implementation can handle this request asynchronously. When hanling is done, the
  /// `done_callback` should be called.
  ///
  /// \param[in] request The request message.
  /// \param[out] reply The reply message.
  /// \param[in] done_callback The callback to be called when the request is done.
  void HandlePushRequest(const PushRequest &request,
                         PushReply *reply,
                         RequestDoneCallback done_callback) = 0;
  void HandlePullRequest(const PullRequest &request,
                         PullReply *reply,
                         RequestDoneCallback done_callback) = 0;
  void HandleFreeObjectsRequest(const FreeObjectsRequest &request,
                         FreeObjectsReply *reply,
                         RequestDoneCallback done_callback) = 0;
};

/// The `GrpcServer` for `ObjectManagerService`.
class ObjectManagerServer : public GrpcServer {
 public:
  /// Construct a `ObjectManagerServer`.
  ///
  /// \param[in] port See `GrpcServer`.
  /// \param[in] handler The service handler that actually handle the requests.
  ObjectManagerServer(const uint32_t port, ObjectManagerServiceHandler &service_handler)
      : GrpcServer("ObjectManager", port), service_handler_(service_handler){};

  void RegisterServices(::grpc::ServerBuilder &builder) override {
    /// Register `ObjectManagerService`.
    builder.RegisterService(&service_);
  }

  void InitServerCallFactories(
      std::vector<std::unique_ptr<ServerCallFactory>> *server_call_factories) override {
    // Initialize the factory for `Push` requests.
    std::unique_ptr<ServerCallFactory> push_call_factory(
        new ServerCallFactoryImpl<ObjectManagerService, ObjectManagerServiceHandler,
                                  PushRequest, ForwardTaskReply>(
            service_, &ObjectManagerService::AsyncService::RequestPush,
            service_handler_, &ObjectManagerServiceHandler::HandleForwardTask, cq_));
    server_call_factories->push_back(std::move(push_call_factory));
    // Initialize the factory for `Pull` requests.
    std::unique_ptr<ServerCallFactory> pull_call_factory(
        new ServerCallFactoryImpl<ObjectManagerService, ObjectManagerServiceHandler,
                                ForwardTaskRequest, ForwardTaskReply>(
          service_, &ObjectManagerService::AsyncService::RequestPull,
          service_handler_, &ObjectManagerServiceHandler::HandleForwardTask, cq_));
    server_call_factories->push_back(std::move(pull_call_factory));
    // Initialize the factory for `FreeObjects` requests.
    std::unique_ptr<ServerCallFactory> free_objects_call_factory(
        new ServerCallFactoryImpl<ObjectManagerService, ObjectManagerServiceHandler,
                              ForwardTaskRequest, ForwardTaskReply>(
        service_, &ObjectManagerService::AsyncService::RequestFreeObjects,
        service_handler_, &ObjectManagerServiceHandler::HandleForwardTask, cq_));
    server_call_factories->push_back(std::move(free_objects_call_factory));
  }

 private:
  /// The grpc async service object.
  ObjectManagerService::AsyncService service_;
  /// The service handler that actually handle the requests.
  ObjectManagerServiceHandler &service_handler_;
};

}  // namespace ray

#endif
