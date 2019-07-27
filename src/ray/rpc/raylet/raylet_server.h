#ifndef RAY_RPC_RAYLET_SERVER_H
#define RAY_RPC_RAYLET_SERVER_H

#include "src/ray/rpc/grpc_server.h"
#include "src/ray/rpc/server_call.h"

#include "src/ray/protobuf/raylet.grpc.pb.h"
#include "src/ray/protobuf/raylet.pb.h"

namespace ray {
namespace rpc {

/// Implementations of the `RayletService`, check interface in
/// `src/ray/protobuf/raylet.proto`.
class RayletServiceHandler {
 public:
  virtual void HandleRegisterClientRequest(const RegisterClientRequest &request,
                                           RegisterClientReply *reply,
                                           SendReplyCallback send_reply_callback) = 0;
  /// Handle a `SubmitTask` request.
  /// The implementation can handle this request asynchronously. When handling is done,
  /// the `send_reply_callback` should be called.
  ///
  /// \param[in] request The request message.
  /// \param[out] reply The reply message.
  /// \param[in] send_reply_callback The callback to be called when the request is done.
  virtual void HandleSubmitTaskRequest(const SubmitTaskRequest &request,
                                       SubmitTaskReply *reply,
                                       SendReplyCallback send_reply_callback) = 0;
  /// Handle a `DisconnectClient` request.
  virtual void HandleDisconnectClientRequest(const DisconnectClientRequest &request,
                                             DisconnectClientReply *reply,
                                             SendReplyCallback send_reply_callback) = 0;
  /// Handle a `GetTask` request.
  virtual void HandleGetTaskRequest(const GetTaskRequest &request, GetTaskReply *reply,
                                    SendReplyCallback send_reply_callback) = 0;
  /// Handle a `TaskDone` request.
  virtual void HandleTaskDoneRequest(const TaskDoneRequest &request, TaskDoneReply *reply,
                                     SendReplyCallback send_reply_callback) = 0;
  /// Handle a `HandleFetchOrReconstruct` request.
  virtual void HandleFetchOrReconstructRequest(const FetchOrReconstructRequest &request,
                                               FetchOrReconstructReply *reply,
                                               SendReplyCallback send_reply_callback) = 0;
  /// Handle a `HandleNotifyUnblocked` request.
  virtual void HandleNotifyUnblockedRequest(const NotifyUnblockedRequest &request,
                                            NotifyUnblockedReply *reply,
                                            SendReplyCallback send_reply_callback) = 0;
  /// Handle a `Wait` request.
  virtual void HandleWaitRequest(const WaitRequest &request, WaitReply *reply,
                                 SendReplyCallback send_reply_callback) = 0;
  /// Handle a `PushError` request.
  virtual void HandlePushErrorRequest(const PushErrorRequest &request,
                                      PushErrorReply *reply,
                                      SendReplyCallback send_reply_callback) = 0;
  /// Handle a `PushProfileEvents` request.
  virtual void HandlePushProfileEventsRequest(const PushProfileEventsRequest &request,
                                              PushProfileEventsReply *reply,
                                              SendReplyCallback send_reply_callback) = 0;
  /// Handle a `FreeObjectsInStoreInObjectStore` request.
  virtual void HandleFreeObjectsInStoreRequest(const FreeObjectsInStoreRequest &request,
                                               FreeObjectsInStoreReply *reply,
                                               SendReplyCallback send_reply_callback) = 0;
  /// Handle a `PrepareActorCheckpoint` request.
  virtual void HandlePrepareActorCheckpointRequest(
      const PrepareActorCheckpointRequest &request, PrepareActorCheckpointReply *reply,
      SendReplyCallback send_reply_callback) = 0;
  /// Handle a `NotifyActorResumedFromCheckpoint` request.
  virtual void HandleNotifyActorResumedFromCheckpointRequest(
      const NotifyActorResumedFromCheckpointRequest &request,
      NotifyActorResumedFromCheckpointReply *reply,
      SendReplyCallback send_reply_callback) = 0;
  /// Handle a `SetResource` request.
  virtual void HandleSetResourceRequest(const SetResourceRequest &request,
                                        SetResourceReply *reply,
                                        SendReplyCallback send_reply_callback) = 0;
  /// Handle a `SetResourceReply` request.
  virtual void HandleHeartbeatRequest(const HeartbeatRequest &request,
                                      HeartbeatReply *reply,
                                      SendReplyCallback send_reply_callback) = 0;
};

/// The `GrpcService` for `RayletGrpcService`.
class RayletGrpcService : public GrpcService {
 public:
  /// Construct a `RayletGrpcService`.
  ///
  /// \param[in] io_service Service used to handle incoming requests
  /// \param[in] handler The service handler that actually handle the requests.
  RayletGrpcService(boost::asio::io_service &io_service,
                    RayletServiceHandler &service_handler)
      : GrpcService(io_service), service_handler_(service_handler){};

 protected:
  grpc::Service &GetGrpcService() override { return service_; }

  void InitServerCallFactories(
      const std::unique_ptr<grpc::ServerCompletionQueue> &cq,
      std::vector<std::pair<std::unique_ptr<ServerCallFactory>, int>>
          *server_call_factories_and_concurrencies) override {
    // Initialize the factory for `RegisterClient` requests.
    std::unique_ptr<ServerCallFactory> register_client_call_factory(
        new ServerCallFactoryImpl<RayletService, RayletServiceHandler,
                                  RegisterClientRequest, RegisterClientReply>(
            service_, &RayletService::AsyncService::RequestRegisterClient,
            service_handler_, &RayletServiceHandler::HandleRegisterClientRequest, cq,
            main_service_));
    server_call_factories_and_concurrencies->emplace_back(
        std::move(register_client_call_factory), 10);

    // Initialize the factory for `SubmitTask` requests.
    std::unique_ptr<ServerCallFactory> submit_task_call_factory(
        new ServerCallFactoryImpl<RayletService, RayletServiceHandler, SubmitTaskRequest,
                                  SubmitTaskReply>(
            service_, &RayletService::AsyncService::RequestSubmitTask, service_handler_,
            &RayletServiceHandler::HandleSubmitTaskRequest, cq, main_service_));
    server_call_factories_and_concurrencies->emplace_back(
        std::move(submit_task_call_factory), 20);

    // Initialize the factory for `DisconnectClient` requests.
    std::unique_ptr<ServerCallFactory> disconnect_client_call_factory(
        new ServerCallFactoryImpl<RayletService, RayletServiceHandler,
                                  DisconnectClientRequest, DisconnectClientReply>(
            service_, &RayletService::AsyncService::RequestDisconnectClient,
            service_handler_, &RayletServiceHandler::HandleDisconnectClientRequest, cq,
            main_service_));
    server_call_factories_and_concurrencies->emplace_back(
        std::move(disconnect_client_call_factory), 10);

    // Initialize the factory for `GetTask` requests.
    std::unique_ptr<ServerCallFactory> get_task_call_factory(
        new ServerCallFactoryImpl<RayletService, RayletServiceHandler, GetTaskRequest,
                                  GetTaskReply>(
            service_, &RayletService::AsyncService::RequestGetTask, service_handler_,
            &RayletServiceHandler::HandleGetTaskRequest, cq, main_service_));
    server_call_factories_and_concurrencies->emplace_back(
        std::move(get_task_call_factory), 20);

    // Initialize the factory for `TaskDone` requests.
    std::unique_ptr<ServerCallFactory> task_done_call_factory(
        new ServerCallFactoryImpl<RayletService, RayletServiceHandler, TaskDoneRequest,
                                  TaskDoneReply>(
            service_, &RayletService::AsyncService::RequestTaskDone, service_handler_,
            &RayletServiceHandler::HandleTaskDoneRequest, cq, main_service_));
    server_call_factories_and_concurrencies->emplace_back(
        std::move(task_done_call_factory), 20);

    // Initialize the factory for `FetchOrReconstruct` requests.
    std::unique_ptr<ServerCallFactory> fetch_or_reconstruct_call_factory(
        new ServerCallFactoryImpl<RayletService, RayletServiceHandler,
                                  FetchOrReconstructRequest, FetchOrReconstructReply>(
            service_, &RayletService::AsyncService::RequestFetchOrReconstruct,
            service_handler_, &RayletServiceHandler::HandleFetchOrReconstructRequest, cq,
            main_service_));
    server_call_factories_and_concurrencies->emplace_back(
        std::move(fetch_or_reconstruct_call_factory), 10);

    // Initialize the factory for `NotifyUnblocked` requests.
    std::unique_ptr<ServerCallFactory> notify_unblocked_call_factory(
        new ServerCallFactoryImpl<RayletService, RayletServiceHandler,
                                  NotifyUnblockedRequest, NotifyUnblockedReply>(
            service_, &RayletService::AsyncService::RequestNotifyUnblocked,
            service_handler_, &RayletServiceHandler::HandleNotifyUnblockedRequest, cq,
            main_service_));
    server_call_factories_and_concurrencies->emplace_back(
        std::move(notify_unblocked_call_factory), 10);

    // Initialize the factory for `Wait` requests.
    std::unique_ptr<ServerCallFactory> wait_call_factory(
        new ServerCallFactoryImpl<RayletService, RayletServiceHandler, WaitRequest,
                                  WaitReply>(
            service_, &RayletService::AsyncService::RequestWait, service_handler_,
            &RayletServiceHandler::HandleWaitRequest, cq, main_service_));
    server_call_factories_and_concurrencies->emplace_back(std::move(wait_call_factory),
                                                          20);

    // Initialize the factory for `PushError` requests.
    std::unique_ptr<ServerCallFactory> push_error_call_factory(
        new ServerCallFactoryImpl<RayletService, RayletServiceHandler, PushErrorRequest,
                                  PushErrorReply>(
            service_, &RayletService::AsyncService::RequestPushError, service_handler_,
            &RayletServiceHandler::HandlePushErrorRequest, cq, main_service_));
    server_call_factories_and_concurrencies->emplace_back(
        std::move(push_error_call_factory), 10);

    // Initialize the factory for `PushProfileEvents` requests.
    std::unique_ptr<ServerCallFactory> push_profile_events_call_factory(
        new ServerCallFactoryImpl<RayletService, RayletServiceHandler,
                                  PushProfileEventsRequest, PushProfileEventsReply>(
            service_, &RayletService::AsyncService::RequestPushProfileEvents,
            service_handler_, &RayletServiceHandler::HandlePushProfileEventsRequest, cq,
            main_service_));
    server_call_factories_and_concurrencies->emplace_back(
        std::move(push_profile_events_call_factory), 10);

    // Initialize the factory for `FreeObjectsInStore` requests.
    std::unique_ptr<ServerCallFactory> free_objects_call_factory(
        new ServerCallFactoryImpl<RayletService, RayletServiceHandler,
                                  FreeObjectsInStoreRequest, FreeObjectsInStoreReply>(
            service_, &RayletService::AsyncService::RequestFreeObjectsInStore,
            service_handler_, &RayletServiceHandler::HandleFreeObjectsInStoreRequest, cq,
            main_service_));
    server_call_factories_and_concurrencies->emplace_back(
        std::move(free_objects_call_factory), 10);

    // Initialize the factory for `PrepareActorCheckpoint` requests.
    std::unique_ptr<ServerCallFactory> prepare_actor_checkpoint_call_factory(
        new ServerCallFactoryImpl<RayletService, RayletServiceHandler,
                                  PrepareActorCheckpointRequest,
                                  PrepareActorCheckpointReply>(
            service_, &RayletService::AsyncService::RequestPrepareActorCheckpoint,
            service_handler_, &RayletServiceHandler::HandlePrepareActorCheckpointRequest,
            cq, main_service_));
    server_call_factories_and_concurrencies->emplace_back(
        std::move(prepare_actor_checkpoint_call_factory), 10);

    // Initialize the factory for `NotifyActorResumedFromCheckpoint` requests.
    std::unique_ptr<ServerCallFactory> notify_actor_resumed_from_checkpoint_call_factory(
        new ServerCallFactoryImpl<RayletService, RayletServiceHandler,
                                  NotifyActorResumedFromCheckpointRequest,
                                  NotifyActorResumedFromCheckpointReply>(
            service_,
            &RayletService::AsyncService::RequestNotifyActorResumedFromCheckpoint,
            service_handler_,
            &RayletServiceHandler::HandleNotifyActorResumedFromCheckpointRequest, cq,
            main_service_));
    server_call_factories_and_concurrencies->emplace_back(
        std::move(notify_actor_resumed_from_checkpoint_call_factory), 10);

    // Initialize the factory for `SetResource` requests.
    std::unique_ptr<ServerCallFactory> set_resource_call_factory(
        new ServerCallFactoryImpl<RayletService, RayletServiceHandler, SetResourceRequest,
                                  SetResourceReply>(
            service_, &RayletService::AsyncService::RequestSetResource, service_handler_,
            &RayletServiceHandler::HandleSetResourceRequest, cq, main_service_));
    server_call_factories_and_concurrencies->emplace_back(
        std::move(set_resource_call_factory), 10);

    // Initialize the factory for `Heartbeat` requests.
    std::unique_ptr<ServerCallFactory> heartbeat_call_factory(
        new ServerCallFactoryImpl<RayletService, RayletServiceHandler, HeartbeatRequest,
                                  HeartbeatReply>(
            service_, &RayletService::AsyncService::RequestHeartbeat, service_handler_,
            &RayletServiceHandler::HandleHeartbeatRequest, cq, main_service_));
    server_call_factories_and_concurrencies->emplace_back(
        std::move(heartbeat_call_factory), 10);
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
