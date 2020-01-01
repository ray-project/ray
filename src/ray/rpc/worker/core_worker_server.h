#ifndef RAY_RPC_CORE_WORKER_SERVER_H
#define RAY_RPC_CORE_WORKER_SERVER_H

#include "ray/rpc/grpc_server.h"
#include "ray/rpc/server_call.h"

#include "src/ray/protobuf/core_worker.grpc.pb.h"
#include "src/ray/protobuf/core_worker.pb.h"

namespace ray {

class CoreWorker;

namespace rpc {

#define RAY_CORE_WORKER_RPC_HANDLERS                                          \
  RPC_SERVICE_HANDLER(CoreWorkerService, AssignTask, 5)                       \
  RPC_SERVICE_HANDLER(CoreWorkerService, PushTask, 9999)                      \
  RPC_SERVICE_HANDLER(CoreWorkerService, DirectActorCallArgWaitComplete, 100) \
  RPC_SERVICE_HANDLER(CoreWorkerService, GetObjectStatus, 9999)               \
  RPC_SERVICE_HANDLER(CoreWorkerService, KillActor, 9999)                     \
  RPC_SERVICE_HANDLER(CoreWorkerService, GetCoreWorkerStats, 100)

/// Interface of the `CoreWorkerServiceHandler`, see `src/ray/protobuf/core_worker.proto`.
class CoreWorkerServiceHandler {
 public:
  /// Handlers. For all of the following handlers, the implementations can
  /// handle the request asynchronously. When handling is done, the
  /// `send_reply_callback` should be called. See
  /// src/ray/rpc/node_manager/node_manager_client.h and
  /// src/ray/protobuf/node_manager.proto for a description of the
  /// functionality of each handler.
  ///
  /// \param[in] request The request message.
  /// \param[out] reply The reply message.
  /// \param[in] send_reply_callback The callback to be called when the request is done.

  virtual void HandleAssignTask(const rpc::AssignTaskRequest &request,
                                rpc::AssignTaskReply *reply,
                                rpc::SendReplyCallback send_reply_callback) = 0;

  virtual void HandlePushTask(const rpc::PushTaskRequest &request,
                              rpc::PushTaskReply *reply,
                              rpc::SendReplyCallback send_reply_callback) = 0;

  virtual void HandleDirectActorCallArgWaitComplete(
      const rpc::DirectActorCallArgWaitCompleteRequest &request,
      rpc::DirectActorCallArgWaitCompleteReply *reply,
      rpc::SendReplyCallback send_reply_callback) = 0;

  virtual void HandleGetObjectStatus(const rpc::GetObjectStatusRequest &request,
                                     rpc::GetObjectStatusReply *reply,
                                     rpc::SendReplyCallback send_reply_callback) = 0;

  virtual void HandleKillActor(const rpc::KillActorRequest &request,
                               rpc::KillActorReply *reply,
                               rpc::SendReplyCallback send_reply_callback) = 0;

  virtual void HandleGetCoreWorkerStats(const rpc::GetCoreWorkerStatsRequest &request,
                                        rpc::GetCoreWorkerStatsReply *reply,
                                        rpc::SendReplyCallback send_reply_callback) = 0;
};

/// The `GrpcServer` for `CoreWorkerService`.
class CoreWorkerGrpcService : public GrpcService {
 public:
  /// Constructor.
  ///
  /// \param[in] main_service See super class.
  /// \param[in] handler The service handler that actually handle the requests.
  CoreWorkerGrpcService(boost::asio::io_service &main_service,
                        CoreWorkerServiceHandler &service_handler)
      : GrpcService(main_service), service_handler_(service_handler) {}

 protected:
  grpc::Service &GetGrpcService() override { return service_; }

  void InitServerCallFactories(
      const std::unique_ptr<grpc::ServerCompletionQueue> &cq,
      std::vector<std::pair<std::unique_ptr<ServerCallFactory>, int>>
          *server_call_factories_and_concurrencies) override {
    RAY_CORE_WORKER_RPC_HANDLERS
  }

 private:
  /// The grpc async service object.
  CoreWorkerService::AsyncService service_;

  /// The service handler that actually handle the requests.
  CoreWorkerServiceHandler &service_handler_;
};

}  // namespace rpc
}  // namespace ray

#endif  // RAY_RPC_CORE_WORKER_SERVER_H
