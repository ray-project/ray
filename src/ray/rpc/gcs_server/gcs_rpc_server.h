#ifndef RAY_RPC_GCS_RPC_SERVER_H
#define RAY_RPC_GCS_RPC_SERVER_H

#include "src/ray/rpc/grpc_server.h"
#include "src/ray/rpc/server_call.h"

#include "src/ray/protobuf/gcs_service.grpc.pb.h"

namespace ray {
namespace rpc {

#define JOB_INFO_SERVICE_RPC_HANDLER(HANDLER, CONCURRENCY) \
  RPC_SERVICE_HANDLER(JobInfoGrpcService, HANDLER, CONCURRENCY)

#define ACTOR_INFO_SERVICE_RPC_HANDLER(HANDLER, CONCURRENCY) \
  RPC_SERVICE_HANDLER(ActorInfoGrpcService, HANDLER, CONCURRENCY)

#define NODE_INFO_SERVICE_RPC_HANDLER(HANDLER, CONCURRENCY) \
  RPC_SERVICE_HANDLER(NodeInfoGrpcService, HANDLER, CONCURRENCY)

#define OBJECT_INFO_SERVICE_RPC_HANDLER(HANDLER, CONCURRENCY) \
  RPC_SERVICE_HANDLER(ObjectInfoGrpcService, HANDLER, CONCURRENCY)

#define TASK_INFO_SERVICE_RPC_HANDLER(HANDLER, CONCURRENCY) \
  RPC_SERVICE_HANDLER(TaskInfoGrpcService, HANDLER, CONCURRENCY)

class JobInfoHandler {
 public:
  virtual ~JobInfoHandler() = default;

  virtual void HandleAddJob(const AddJobRequest &request, AddJobReply *reply,
                            SendReplyCallback send_reply_callback) = 0;

  virtual void HandleMarkJobFinished(const MarkJobFinishedRequest &request,
                                     MarkJobFinishedReply *reply,
                                     SendReplyCallback send_reply_callback) = 0;
};

/// The `GrpcService` for `JobInfoGcsService`.
class JobInfoGrpcService : public GrpcService {
 public:
  /// Constructor.
  ///
  /// \param[in] handler The service handler that actually handle the requests.
  explicit JobInfoGrpcService(boost::asio::io_service &io_service,
                              JobInfoHandler &handler)
      : GrpcService(io_service), service_handler_(handler){};

 protected:
  grpc::Service &GetGrpcService() override { return service_; }

  void InitServerCallFactories(
      const std::unique_ptr<grpc::ServerCompletionQueue> &cq,
      std::vector<std::pair<std::unique_ptr<ServerCallFactory>, int>>
          *server_call_factories_and_concurrencies) override {
    RPC_SERVICE_HANDLER(JobInfoGrpcService, AddJob, 1);
    RPC_SERVICE_HANDLER(JobInfoGrpcService, MarkJobFinished, 1);
  }

 private:
  /// The grpc async service object.
  JobInfoGcsService::AsyncService service_;
  /// The service handler that actually handle the requests.
  JobInfoHandler &service_handler_;
};

class ActorInfoHandler {
 public:
  virtual ~ActorInfoHandler() = default;

  virtual void HandleGetActorInfo(const GetActorInfoRequest &request,
                                  GetActorInfoReply *reply,
                                  SendReplyCallback send_reply_callback) = 0;

  virtual void HandleRegisterActorInfo(const RegisterActorInfoRequest &request,
                                       RegisterActorInfoReply *reply,
                                       SendReplyCallback send_reply_callback) = 0;

  virtual void HandleUpdateActorInfo(const UpdateActorInfoRequest &request,
                                     UpdateActorInfoReply *reply,
                                     SendReplyCallback send_reply_callback) = 0;

  virtual void HandleAddActorCheckpoint(const AddActorCheckpointRequest &request,
                                        AddActorCheckpointReply *reply,
                                        SendReplyCallback send_reply_callback) = 0;

  virtual void HandleGetActorCheckpoint(const GetActorCheckpointRequest &request,
                                        GetActorCheckpointReply *reply,
                                        SendReplyCallback send_reply_callback) = 0;

  virtual void HandleGetActorCheckpointID(const GetActorCheckpointIDRequest &request,
                                          GetActorCheckpointIDReply *reply,
                                          SendReplyCallback send_reply_callback) = 0;
};

/// The `GrpcService` for `ActorInfoGcsService`.
class ActorInfoGrpcService : public GrpcService {
 public:
  /// Constructor.
  ///
  /// \param[in] handler The service handler that actually handle the requests.
  explicit ActorInfoGrpcService(boost::asio::io_service &io_service,
                                ActorInfoHandler &handler)
      : GrpcService(io_service), service_handler_(handler){};

 protected:
  grpc::Service &GetGrpcService() override { return service_; }

  void InitServerCallFactories(
      const std::unique_ptr<grpc::ServerCompletionQueue> &cq,
      std::vector<std::pair<std::unique_ptr<ServerCallFactory>, int>>
          *server_call_factories_and_concurrencies) override {
    ACTOR_INFO_SERVICE_RPC_HANDLER(GetActorInfo, 1);
    ACTOR_INFO_SERVICE_RPC_HANDLER(RegisterActorInfo, 1);
    ACTOR_INFO_SERVICE_RPC_HANDLER(UpdateActorInfo, 1);
    ACTOR_INFO_SERVICE_RPC_HANDLER(AddActorCheckpoint, 1);
    ACTOR_INFO_SERVICE_RPC_HANDLER(GetActorCheckpoint, 1);
    ACTOR_INFO_SERVICE_RPC_HANDLER(GetActorCheckpointID, 1);
  }

 private:
  /// The grpc async service object.
  ActorInfoGcsService::AsyncService service_;
  /// The service handler that actually handle the requests.
  ActorInfoHandler &service_handler_;
};

class NodeInfoHandler {
 public:
  virtual ~NodeInfoHandler() = default;

  virtual void HandleRegisterNode(const RegisterNodeRequest &request,
                                  RegisterNodeReply *reply,
                                  SendReplyCallback send_reply_callback) = 0;

  virtual void HandleUnregisterNode(const UnregisterNodeRequest &request,
                                    UnregisterNodeReply *reply,
                                    SendReplyCallback send_reply_callback) = 0;

  virtual void HandleGetAllNodeInfo(const GetAllNodeInfoRequest &request,
                                    GetAllNodeInfoReply *reply,
                                    SendReplyCallback send_reply_callback) = 0;

  virtual void HandleReportHeartbeat(const ReportHeartbeatRequest &request,
                                     ReportHeartbeatReply *reply,
                                     SendReplyCallback send_reply_callback) = 0;

  virtual void HandleReportBatchHeartbeat(const ReportBatchHeartbeatRequest &request,
                                          ReportBatchHeartbeatReply *reply,
                                          SendReplyCallback send_reply_callback) = 0;

  virtual void HandleGetResources(const GetResourcesRequest &request,
                                  GetResourcesReply *reply,
                                  SendReplyCallback send_reply_callback) = 0;

  virtual void HandleUpdateResources(const UpdateResourcesRequest &request,
                                     UpdateResourcesReply *reply,
                                     SendReplyCallback send_reply_callback) = 0;

  virtual void HandleDeleteResources(const DeleteResourcesRequest &request,
                                     DeleteResourcesReply *reply,
                                     SendReplyCallback send_reply_callback) = 0;
};

/// The `GrpcService` for `NodeInfoGcsService`.
class NodeInfoGrpcService : public GrpcService {
 public:
  /// Constructor.
  ///
  /// \param[in] handler The service handler that actually handle the requests.
  explicit NodeInfoGrpcService(boost::asio::io_service &io_service,
                               NodeInfoHandler &handler)
      : GrpcService(io_service), service_handler_(handler){};

 protected:
  grpc::Service &GetGrpcService() override { return service_; }

  void InitServerCallFactories(
      const std::unique_ptr<grpc::ServerCompletionQueue> &cq,
      std::vector<std::pair<std::unique_ptr<ServerCallFactory>, int>>
          *server_call_factories_and_concurrencies) override {
    NODE_INFO_SERVICE_RPC_HANDLER(RegisterNode, 1);
    NODE_INFO_SERVICE_RPC_HANDLER(UnregisterNode, 1);
    NODE_INFO_SERVICE_RPC_HANDLER(GetAllNodeInfo, 1);
    NODE_INFO_SERVICE_RPC_HANDLER(ReportHeartbeat, 1);
    NODE_INFO_SERVICE_RPC_HANDLER(ReportBatchHeartbeat, 1);
    NODE_INFO_SERVICE_RPC_HANDLER(GetResources, 1);
    NODE_INFO_SERVICE_RPC_HANDLER(UpdateResources, 1);
    NODE_INFO_SERVICE_RPC_HANDLER(DeleteResources, 1);
  }

 private:
  /// The grpc async service object.
  NodeInfoGcsService::AsyncService service_;
  /// The service handler that actually handle the requests.
  NodeInfoHandler &service_handler_;
};

class ObjectInfoHandler {
 public:
  virtual ~ObjectInfoHandler() = default;

  virtual void HandleGetObjectLocations(const GetObjectLocationsRequest &request,
                                        GetObjectLocationsReply *reply,
                                        SendReplyCallback send_reply_callback) = 0;

  virtual void HandleAddObjectLocation(const AddObjectLocationRequest &request,
                                       AddObjectLocationReply *reply,
                                       SendReplyCallback send_reply_callback) = 0;

  virtual void HandleRemoveObjectLocation(const RemoveObjectLocationRequest &request,
                                          RemoveObjectLocationReply *reply,
                                          SendReplyCallback send_reply_callback) = 0;
};

/// The `GrpcService` for `ObjectInfoHandler`.
class ObjectInfoGrpcService : public GrpcService {
 public:
  /// Constructor.
  ///
  /// \param[in] handler The service handler that actually handle the requests.
  explicit ObjectInfoGrpcService(boost::asio::io_service &io_service,
                                 ObjectInfoHandler &handler)
      : GrpcService(io_service), service_handler_(handler){};

 protected:
  grpc::Service &GetGrpcService() override { return service_; }

  void InitServerCallFactories(
      const std::unique_ptr<grpc::ServerCompletionQueue> &cq,
      std::vector<std::pair<std::unique_ptr<ServerCallFactory>, int>>
          *server_call_factories_and_concurrencies) override {
    RPC_SERVICE_HANDLER(ObjectInfoGrpcService, GetObjectLocations, 1);
    RPC_SERVICE_HANDLER(ObjectInfoGrpcService, AddObjectLocation, 1);
    RPC_SERVICE_HANDLER(ObjectInfoGrpcService, RemoveObjectLocation, 1);
  }

 private:
  /// The grpc async service object.
  ObjectInfoGcsService::AsyncService service_;
  /// The service handler that actually handle the requests.
  ObjectInfoHandler &service_handler_;
};

class TaskInfoHandler {
 public:
  virtual ~TaskInfoHandler() = default;

  virtual void HandleAddTask(const AddTaskRequest &request, AddTaskReply *reply,
                             SendReplyCallback send_reply_callback) = 0;

  virtual void HandleGetTask(const GetTaskRequest &request, GetTaskReply *reply,
                             SendReplyCallback send_reply_callback) = 0;

  virtual void HandleDeleteTasks(const DeleteTasksRequest &request,
                                 DeleteTasksReply *reply,
                                 SendReplyCallback send_reply_callback) = 0;
};

/// The `GrpcService` for `TaskInfoGcsService`.
class TaskInfoGrpcService : public GrpcService {
 public:
  /// Constructor.
  ///
  /// \param[in] handler The service handler that actually handle the requests.
  explicit TaskInfoGrpcService(boost::asio::io_service &io_service,
                               TaskInfoHandler &handler)
      : GrpcService(io_service), service_handler_(handler){};

 protected:
  grpc::Service &GetGrpcService() override { return service_; }

  void InitServerCallFactories(
      const std::unique_ptr<grpc::ServerCompletionQueue> &cq,
      std::vector<std::pair<std::unique_ptr<ServerCallFactory>, int>>
          *server_call_factories_and_concurrencies) override {
    TASK_INFO_SERVICE_RPC_HANDLER(AddTask, 1);
    TASK_INFO_SERVICE_RPC_HANDLER(GetTask, 1);
    TASK_INFO_SERVICE_RPC_HANDLER(DeleteTasks, 1);
  }

 private:
  /// The grpc async service object.
  TaskInfoGcsService::AsyncService service_;
  /// The service handler that actually handle the requests.
  TaskInfoHandler &service_handler_;
};

}  // namespace rpc
}  // namespace ray

#endif  // RAY_RPC_GCS_RPC_SERVER_H
