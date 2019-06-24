#ifndef RAY_RPC_WORKER_SERVER_H
#define RAY_RPC_WORKER_SERVER_H

#include "ray/rpc/grpc_server.h"
#include "ray/rpc/server_call.h"

#include "src/ray/protobuf/worker.grpc.pb.h"
#include "src/ray/protobuf/worker.pb.h"

namespace ray {
namespace rpc {

/// Interface of the `WorkerService`, see `src/ray/protobuf/worker.proto`.
class WorkerServiceHandler {
 public:
  /// Handle a `PushTask` request.
  /// The implementation can handle this request asynchronously. When hanling is done, the
  /// `done_callback` should be called.
  ///
  /// \param[in] request The request message.
  /// \param[out] reply The reply message.
  /// \param[in] done_callback The callback to be called when the request is done.
  virtual void HandlePushTask(const PushTaskRequest &request,
                                 PushTaskReply *reply,
                                 RequestDoneCallback done_callback) = 0;
};

/// The `GrpcServer` for `WorkerService`.
class WorkerServer : public GrpcServer {
 public:
  /// Constructor.
  ///
  /// \param[in] port See super class.
  /// \param[in] main_service See super class.
  /// \param[in] handler The service handler that actually handle the requests.
  WorkerServer(const uint32_t port, boost::asio::io_service &main_service,
                    WorkerServiceHandler &service_handler)
      : GrpcServer("Worker", port, main_service),
        service_handler_(service_handler){};

  void RegisterServices(grpc::ServerBuilder &builder) override {
    /// Register `WorkerService`.
    builder.RegisterService(&service_);
  }

  void InitServerCallFactories(
      std::vector<std::pair<std::unique_ptr<ServerCallFactory>, int>>
          *server_call_factories_and_concurrencies) override {
    // Initialize the Factory for `PushTask` requests.
    std::unique_ptr<ServerCallFactory> push_task_call_Factory(
        new ServerCallFactoryImpl<WorkerService, WorkerServiceHandler,
                                  PushTaskRequest, PushTaskReply>(
            service_, &WorkerService::AsyncService::RequestPushTask,
            service_handler_, &WorkerServiceHandler::HandlePushTask, cq_));

    // Set `PushTask`'s accept concurrency to 100.
    server_call_factories_and_concurrencies->emplace_back(
        std::move(push_task_call_Factory), 100);
  }

 private:
  /// The grpc async service object.
  WorkerService::AsyncService service_;
  /// The service handler that actually handle the requests.
  WorkerServiceHandler &service_handler_;
};

}  // namespace rpc
}  // namespace ray

#endif
