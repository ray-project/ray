#ifndef RAY_RPC_WORKER_SERVER_H
#define RAY_RPC_WORKER_SERVER_H

#include "ray/rpc/asio_server.h"
#include "ray/rpc/grpc_server.h"
#include "ray/rpc/server_call.h"

#include "src/ray/protobuf/worker.grpc.pb.h"
#include "src/ray/protobuf/worker.pb.h"

namespace ray {
namespace rpc {

/// Interface of the `WorkerService`, see `src/ray/protobuf/worker.proto`.
class WorkerTaskHandler {
 public:
  /// Handle a `AssignTask` request.
  /// The implementation can handle this request asynchronously. When handling is done,
  /// the `send_reply_callback` should be called.
  ///
  /// \param[in] request The request message.
  /// \param[out] reply The reply message.
  /// \param[in] send_reply_callback The callback to be called when the request is done.
  virtual void HandleAssignTask(const AssignTaskRequest &request, AssignTaskReply *reply,
                                SendReplyCallback send_reply_callback) = 0;
};

/// The `GrpcServer` for `WorkerService`.
class WorkerTaskGrpcService : public GrpcService {
 public:
  /// Constructor.
  ///
  /// \param[in] main_service See super class.
  /// \param[in] handler The service handler that actually handle the requests.
  WorkerTaskGrpcService(boost::asio::io_service &main_service,
                        WorkerTaskHandler &service_handler)
      : GrpcService(main_service), service_handler_(service_handler){};

 protected:
  grpc::Service &GetGrpcService() override { return service_; }

  void InitServerCallFactories(
      const std::unique_ptr<grpc::ServerCompletionQueue> &cq,
      std::vector<std::pair<std::unique_ptr<ServerCallFactory>, int>>
          *server_call_factories_and_concurrencies) override {
    // Initialize the Factory for `AssignTask` requests.
    std::unique_ptr<ServerCallFactory> push_task_call_Factory(
        new ServerCallFactoryImpl<WorkerTaskService, WorkerTaskHandler, AssignTaskRequest,
                                  AssignTaskReply>(
            service_, &WorkerTaskService::AsyncService::RequestAssignTask,
            service_handler_, &WorkerTaskHandler::HandleAssignTask, cq, main_service_));

    // Set `AssignTask`'s accept concurrency to 5.
    server_call_factories_and_concurrencies->emplace_back(
        std::move(push_task_call_Factory), 5);
  }

 private:
  /// The grpc async service object.
  WorkerTaskService::AsyncService service_;

  /// The service handler that actually handle the requests.
  WorkerTaskHandler &service_handler_;
};

/// The `AsioRpcService` for `WorkerTaskService`.
class WorkerTaskAsioRpcService : public AsioRpcService {
 public:
  /// Constructor.
  ///
  /// \param[in] main_service See super class.
  /// \param[in] handler The service handler that actually handle the requests.
  WorkerTaskAsioRpcService(WorkerTaskHandler &service_handler)
      : AsioRpcService(rpc::RpcServiceType::WorkerTaskServiceType),
        service_handler_(service_handler){};

 protected:
  void InitMethodHandlers(
      std::vector<std::shared_ptr<ServiceMethod>> *server_call_methods,
      std::vector<std::string> *message_type_enum_names) override {
    // Initialize the Factory for `PushTask` requests.
    std::shared_ptr<ServiceMethod> assign_task_call_method(
        new ServiceMethodImpl<WorkerTaskHandler, AssignTaskRequest, AssignTaskReply,
                              WorkerTaskServiceMessageType>(
            service_type_, WorkerTaskServiceMessageType::AssignTaskRequestMessage,
            WorkerTaskServiceMessageType::AssignTaskReplytMessage, service_handler_,
            &WorkerTaskHandler::HandleAssignTask));

    server_call_methods->emplace_back(std::move(assign_task_call_method));

    *message_type_enum_names = GenerateEnumNames(WorkerTaskServiceMessageType);
  }

 private:
  /// The service handler that actually handle the requests.
  WorkerTaskHandler &service_handler_;
};

}  // namespace rpc
}  // namespace ray

#endif
