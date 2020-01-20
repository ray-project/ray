#ifndef RAY_RPC_GCS_RPC_SERVER_H
#define RAY_RPC_GCS_RPC_SERVER_H

#include "src/ray/rpc/grpc_server.h"
#include "src/ray/rpc/server_call.h"

#include "src/ray/protobuf/gcs_service.grpc.pb.h"

namespace ray {
namespace rpc {

#define JOB_INFO_SERVICE_RPC_HANDLER(HANDLER, CONCURRENCY) \
  RPC_SERVICE_HANDLER(JobInfoGcsService, HANDLER, CONCURRENCY)

#define ACTOR_INFO_SERVICE_RPC_HANDLER(HANDLER, CONCURRENCY) \
  RPC_SERVICE_HANDLER(ActorInfoGcsService, HANDLER, CONCURRENCY)

#define NODE_INFO_SERVICE_RPC_HANDLER(HANDLER, CONCURRENCY) \
  RPC_SERVICE_HANDLER(NodeInfoGcsService, HANDLER, CONCURRENCY)

#define OBJECT_INFO_SERVICE_RPC_HANDLER(HANDLER, CONCURRENCY) \
  RPC_SERVICE_HANDLER(ObjectInfoGcsService, HANDLER, CONCURRENCY)

#define TASK_INFO_SERVICE_RPC_HANDLER(HANDLER, CONCURRENCY) \
  RPC_SERVICE_HANDLER(TaskInfoGcsService, HANDLER, CONCURRENCY)

#define STATS_SERVICE_RPC_HANDLER(HANDLER, CONCURRENCY) \
  RPC_SERVICE_HANDLER(StatsGcsService, HANDLER, CONCURRENCY)

class JobInfoGcsServiceHandler {
 public:
  virtual ~JobInfoGcsServiceHandler() = default;

  DECLARE_VOID_RPC_SERVICE_HANDLER_METHOD(AddJob)
  DECLARE_VOID_RPC_SERVICE_HANDLER_METHOD(MarkJobFinished)
};

/// The `GrpcService` for `JobInfoGcsService`.
class JobInfoGrpcService : public GrpcService {
 public:
  /// Constructor.
  ///
  /// \param[in] handler The service handler that actually handle the requests.
  explicit JobInfoGrpcService(boost::asio::io_service &io_service,
                              JobInfoGcsServiceHandler &handler)
      : GrpcService(io_service), service_handler_(handler){};

 protected:
  grpc::Service &GetGrpcService() override { return service_; }

  void InitServerCallFactories(
      const std::unique_ptr<grpc::ServerCompletionQueue> &cq,
      std::vector<std::pair<std::unique_ptr<ServerCallFactory>, int>>
          *server_call_factories_and_concurrencies) override {
    JOB_INFO_SERVICE_RPC_HANDLER(AddJob, 1);
    JOB_INFO_SERVICE_RPC_HANDLER(MarkJobFinished, 1);
  }

 private:
  /// The grpc async service object.
  JobInfoGcsService::AsyncService service_;
  /// The service handler that actually handle the requests.
  JobInfoGcsServiceHandler &service_handler_;
};

class ActorInfoGcsServiceHandler {
 public:
  virtual ~ActorInfoGcsServiceHandler() = default;

  DECLARE_VOID_RPC_SERVICE_HANDLER_METHOD(GetActorInfo)
  DECLARE_VOID_RPC_SERVICE_HANDLER_METHOD(RegisterActorInfo)
  DECLARE_VOID_RPC_SERVICE_HANDLER_METHOD(UpdateActorInfo)
  DECLARE_VOID_RPC_SERVICE_HANDLER_METHOD(AddActorCheckpoint)
  DECLARE_VOID_RPC_SERVICE_HANDLER_METHOD(GetActorCheckpoint)
  DECLARE_VOID_RPC_SERVICE_HANDLER_METHOD(GetActorCheckpointID)
};

/// The `GrpcService` for `ActorInfoGcsService`.
class ActorInfoGrpcService : public GrpcService {
 public:
  /// Constructor.
  ///
  /// \param[in] handler The service handler that actually handle the requests.
  explicit ActorInfoGrpcService(boost::asio::io_service &io_service,
                                ActorInfoGcsServiceHandler &handler)
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
  ActorInfoGcsServiceHandler &service_handler_;
};

class NodeInfoGcsServiceHandler {
 public:
  virtual ~NodeInfoGcsServiceHandler() = default;

  DECLARE_VOID_RPC_SERVICE_HANDLER_METHOD(RegisterNode)
  DECLARE_VOID_RPC_SERVICE_HANDLER_METHOD(UnregisterNode)
  DECLARE_VOID_RPC_SERVICE_HANDLER_METHOD(GetAllNodeInfo)
  DECLARE_VOID_RPC_SERVICE_HANDLER_METHOD(ReportHeartbeat)
  DECLARE_VOID_RPC_SERVICE_HANDLER_METHOD(ReportBatchHeartbeat)
  DECLARE_VOID_RPC_SERVICE_HANDLER_METHOD(GetResources)
  DECLARE_VOID_RPC_SERVICE_HANDLER_METHOD(UpdateResources)
  DECLARE_VOID_RPC_SERVICE_HANDLER_METHOD(DeleteResources)
};

/// The `GrpcService` for `NodeInfoGcsService`.
class NodeInfoGrpcService : public GrpcService {
 public:
  /// Constructor.
  ///
  /// \param[in] handler The service handler that actually handle the requests.
  explicit NodeInfoGrpcService(boost::asio::io_service &io_service,
                               NodeInfoGcsServiceHandler &handler)
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
  NodeInfoGcsServiceHandler &service_handler_;
};

class ObjectInfoGcsServiceHandler {
 public:
  virtual ~ObjectInfoGcsServiceHandler() = default;

  DECLARE_VOID_RPC_SERVICE_HANDLER_METHOD(GetObjectLocations)
  DECLARE_VOID_RPC_SERVICE_HANDLER_METHOD(AddObjectLocation)
  DECLARE_VOID_RPC_SERVICE_HANDLER_METHOD(RemoveObjectLocation)
};

/// The `GrpcService` for `ObjectInfoGcsServiceHandler`.
class ObjectInfoGrpcService : public GrpcService {
 public:
  /// Constructor.
  ///
  /// \param[in] handler The service handler that actually handle the requests.
  explicit ObjectInfoGrpcService(boost::asio::io_service &io_service,
                                 ObjectInfoGcsServiceHandler &handler)
      : GrpcService(io_service), service_handler_(handler){};

 protected:
  grpc::Service &GetGrpcService() override { return service_; }

  void InitServerCallFactories(
      const std::unique_ptr<grpc::ServerCompletionQueue> &cq,
      std::vector<std::pair<std::unique_ptr<ServerCallFactory>, int>>
          *server_call_factories_and_concurrencies) override {
    OBJECT_INFO_SERVICE_RPC_HANDLER(GetObjectLocations, 1);
    OBJECT_INFO_SERVICE_RPC_HANDLER(AddObjectLocation, 1);
    OBJECT_INFO_SERVICE_RPC_HANDLER(RemoveObjectLocation, 1);
  }

 private:
  /// The grpc async service object.
  ObjectInfoGcsService::AsyncService service_;
  /// The service handler that actually handle the requests.
  ObjectInfoGcsServiceHandler &service_handler_;
};

class TaskInfoGcsServiceHandler {
 public:
  virtual ~TaskInfoGcsServiceHandler() = default;

  DECLARE_VOID_RPC_SERVICE_HANDLER_METHOD(AddTask)
  DECLARE_VOID_RPC_SERVICE_HANDLER_METHOD(GetTask)
  DECLARE_VOID_RPC_SERVICE_HANDLER_METHOD(DeleteTasks)
  DECLARE_VOID_RPC_SERVICE_HANDLER_METHOD(AddTaskLease)
  DECLARE_VOID_RPC_SERVICE_HANDLER_METHOD(AttemptTaskReconstruction)
};

/// The `GrpcService` for `TaskInfoGcsService`.
class TaskInfoGrpcService : public GrpcService {
 public:
  /// Constructor.
  ///
  /// \param[in] handler The service handler that actually handle the requests.
  explicit TaskInfoGrpcService(boost::asio::io_service &io_service,
                               TaskInfoGcsServiceHandler &handler)
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
    TASK_INFO_SERVICE_RPC_HANDLER(AddTaskLease, 1);
    TASK_INFO_SERVICE_RPC_HANDLER(AttemptTaskReconstruction, 1);
  }

 private:
  /// The grpc async service object.
  TaskInfoGcsService::AsyncService service_;
  /// The service handler that actually handle the requests.
  TaskInfoGcsServiceHandler &service_handler_;
};

class StatsGcsServiceHandler {
 public:
  virtual ~StatsGcsServiceHandler() = default;

  DECLARE_VOID_RPC_SERVICE_HANDLER_METHOD(AddProfileData)
};

/// The `GrpcService` for `StatsGcsService`.
class StatsGrpcService : public GrpcService {
 public:
  /// Constructor.
  ///
  /// \param[in] handler The service handler that actually handle the requests.
  explicit StatsGrpcService(boost::asio::io_service &io_service,
                            StatsGcsServiceHandler &handler)
      : GrpcService(io_service), service_handler_(handler){};

 protected:
  grpc::Service &GetGrpcService() override { return service_; }

  void InitServerCallFactories(
      const std::unique_ptr<grpc::ServerCompletionQueue> &cq,
      std::vector<std::pair<std::unique_ptr<ServerCallFactory>, int>>
          *server_call_factories_and_concurrencies) override {
    STATS_SERVICE_RPC_HANDLER(AddProfileData, 1);
  }

 private:
  /// The grpc async service object.
  StatsGcsService::AsyncService service_;
  /// The service handler that actually handle the requests.
  StatsGcsServiceHandler &service_handler_;
};

using JobInfoHandler = JobInfoGcsServiceHandler;
using ActorInfoHandler = ActorInfoGcsServiceHandler;
using NodeInfoHandler = NodeInfoGcsServiceHandler;
using ObjectInfoHandler = ObjectInfoGcsServiceHandler;
using TaskInfoHandler = TaskInfoGcsServiceHandler;
using StatsHandler = StatsGcsServiceHandler;

}  // namespace rpc
}  // namespace ray

#endif  // RAY_RPC_GCS_RPC_SERVER_H
