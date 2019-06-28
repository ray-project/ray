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
  virtual void HandleRegisterClientRequest(const RegisterClientRequest &request,
                                           RegisterClientReply *reply,
                                           RequestDoneCallback done_callback) = 0;
  /// Handle a `SubmitTask` request.
  /// The implementation can handle this request asynchronously. When handling is done,
  /// the `done_callback` should be called.
  ///
  /// \param[in] request The request message.
  /// \param[out] reply The reply message.
  /// \param[in] done_callback The callback to be called when the request is done.
  virtual void HandleSubmitTaskRequest(const SubmitTaskRequest &request,
                                       SubmitTaskReply *reply,
                                       RequestDoneCallback done_callback) = 0;
  /// Handle a `TaskDone` request.
  virtual void HandleTaskDoneRequest(const TaskDoneRequest &request, TaskDoneReply *reply,
                                     RequestDoneCallback done_callback) = 0;
  /// Handle a `EventLog` request.
  virtual void HandleEventLogRequest(const EventLogRequest &request, EventLogReply *reply,
                                     RequestDoneCallback done_callback) = 0;
  /// Handle a `GetTask` request.
  virtual void HandleGetTaskRequest(const GetTaskRequest &request, GetTaskReply *reply,
                                    RequestDoneCallback done_callback) = 0;
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
    // Initialize the factory for `SubmitTask` requests.
    std::unique_ptr<ServerCallFactory> submit_task_call_factory(
        new ServerCallFactoryImpl<RayletService, RayletServiceHandler, SubmitTaskRequest,
                                  SubmitTaskReply>(
            service_, &RayletService::AsyncService::RequestSubmitTask, service_handler_,
            &RayletServiceHandler::HandleSubmitTaskRequest, cq, main_service_));
    server_call_factories_and_concurrencies->emplace_back(
        std::move(submit_task_call_factory), 10);

    // Initialize the factory for `TaskDone` requests.
    std::unique_ptr<ServerCallFactory> task_done_call_factory(
        new ServerCallFactoryImpl<RayletService, RayletServiceHandler, TaskDoneRequest,
                                  TaskDoneReply>(
            service_, &RayletService::AsyncService::RequestTaskDone, service_handler_,
            &RayletServiceHandler::HandleTaskDoneRequest, cq, main_service_));
    server_call_factories_and_concurrencies->emplace_back(
        std::move(task_done_call_factory), 10);

    // Initialize the factory for `EventLog` requests.
    std::unique_ptr<ServerCallFactory> event_log_call_factory(
        new ServerCallFactoryImpl<RayletService, RayletServiceHandler, EventLogRequest,
                                  EventLogReply>(
            service_, &RayletService::AsyncService::RequestEventLog, service_handler_,
            &RayletServiceHandler::HandleEventLogRequest, cq, main_service_));
    server_call_factories_and_concurrencies->emplace_back(
        std::move(event_log_call_factory), 10);

    // Initialize the factory for `GetTask` requests.
    std::unique_ptr<ServerCallFactory> get_task_call_factory(
        new ServerCallFactoryImpl<RayletService, RayletServiceHandler, GetTaskRequest,
                                  EventLogReply>(
            service_, &RayletService::AsyncService::RequestGetTask, service_handler_,
            &RayletServiceHandler::GetTaskRequest, cq, main_service_));
    server_call_factories_and_concurrencies->emplace_back(
        std::move(get_task_call_factory), 10);
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
