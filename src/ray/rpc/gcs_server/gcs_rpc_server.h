// Copyright 2017 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include "ray/rpc/grpc_server.h"
#include "ray/rpc/server_call.h"
#include "src/ray/protobuf/gcs_service.grpc.pb.h"

namespace ray {
namespace rpc {

#define JOB_INFO_SERVICE_RPC_HANDLER(HANDLER) \
  RPC_SERVICE_HANDLER(JobInfoGcsService, HANDLER)

#define ACTOR_INFO_SERVICE_RPC_HANDLER(HANDLER) \
  RPC_SERVICE_HANDLER(ActorInfoGcsService, HANDLER)

#define NODE_INFO_SERVICE_RPC_HANDLER(HANDLER) \
  RPC_SERVICE_HANDLER(NodeInfoGcsService, HANDLER)

#define HEARTBEAT_INFO_SERVICE_RPC_HANDLER(HANDLER) \
  RPC_SERVICE_HANDLER(HeartbeatInfoGcsService, HANDLER)

#define NODE_RESOURCE_INFO_SERVICE_RPC_HANDLER(HANDLER) \
  RPC_SERVICE_HANDLER(NodeResourceInfoGcsService, HANDLER)

#define OBJECT_INFO_SERVICE_RPC_HANDLER(HANDLER) \
  RPC_SERVICE_HANDLER(ObjectInfoGcsService, HANDLER)

#define TASK_INFO_SERVICE_RPC_HANDLER(HANDLER) \
  RPC_SERVICE_HANDLER(TaskInfoGcsService, HANDLER)

#define STATS_SERVICE_RPC_HANDLER(HANDLER) RPC_SERVICE_HANDLER(StatsGcsService, HANDLER)

#define WORKER_INFO_SERVICE_RPC_HANDLER(HANDLER) \
  RPC_SERVICE_HANDLER(WorkerInfoGcsService, HANDLER)

#define PLACEMENT_GROUP_INFO_SERVICE_RPC_HANDLER(HANDLER) \
  RPC_SERVICE_HANDLER(PlacementGroupInfoGcsService, HANDLER)

#define GCS_RPC_SEND_REPLY(send_reply_callback, reply, status) \
  reply->mutable_status()->set_code((int)status.code());       \
  reply->mutable_status()->set_message(status.message());      \
  send_reply_callback(ray::Status::OK(), nullptr, nullptr)

class JobInfoGcsServiceHandler {
 public:
  virtual ~JobInfoGcsServiceHandler() = default;

  virtual void HandleAddJob(const AddJobRequest &request, AddJobReply *reply,
                            SendReplyCallback send_reply_callback) = 0;

  virtual void HandleMarkJobFinished(const MarkJobFinishedRequest &request,
                                     MarkJobFinishedReply *reply,
                                     SendReplyCallback send_reply_callback) = 0;

  virtual void HandleGetAllJobInfo(const GetAllJobInfoRequest &request,
                                   GetAllJobInfoReply *reply,
                                   SendReplyCallback send_reply_callback) = 0;

  virtual void AddJobFinishedListener(
      std::function<void(std::shared_ptr<JobID>)> listener) = 0;
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
      std::vector<std::unique_ptr<ServerCallFactory>> *server_call_factories) override {
    JOB_INFO_SERVICE_RPC_HANDLER(AddJob);
    JOB_INFO_SERVICE_RPC_HANDLER(MarkJobFinished);
    JOB_INFO_SERVICE_RPC_HANDLER(GetAllJobInfo);
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

  virtual void HandleRegisterActor(const RegisterActorRequest &request,
                                   RegisterActorReply *reply,
                                   SendReplyCallback send_reply_callback) = 0;

  virtual void HandleCreateActor(const CreateActorRequest &request,
                                 CreateActorReply *reply,
                                 SendReplyCallback send_reply_callback) = 0;

  virtual void HandleGetActorInfo(const GetActorInfoRequest &request,
                                  GetActorInfoReply *reply,
                                  SendReplyCallback send_reply_callback) = 0;

  virtual void HandleGetNamedActorInfo(const GetNamedActorInfoRequest &request,
                                       GetNamedActorInfoReply *reply,
                                       SendReplyCallback send_reply_callback) = 0;

  virtual void HandleGetAllActorInfo(const GetAllActorInfoRequest &request,
                                     GetAllActorInfoReply *reply,
                                     SendReplyCallback send_reply_callback) = 0;
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
      std::vector<std::unique_ptr<ServerCallFactory>> *server_call_factories) override {
    ACTOR_INFO_SERVICE_RPC_HANDLER(RegisterActor);
    ACTOR_INFO_SERVICE_RPC_HANDLER(CreateActor);
    ACTOR_INFO_SERVICE_RPC_HANDLER(GetActorInfo);
    ACTOR_INFO_SERVICE_RPC_HANDLER(GetNamedActorInfo);
    ACTOR_INFO_SERVICE_RPC_HANDLER(GetAllActorInfo);
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

  virtual void HandleRegisterNode(const RegisterNodeRequest &request,
                                  RegisterNodeReply *reply,
                                  SendReplyCallback send_reply_callback) = 0;

  virtual void HandleUnregisterNode(const UnregisterNodeRequest &request,
                                    UnregisterNodeReply *reply,
                                    SendReplyCallback send_reply_callback) = 0;

  virtual void HandleGetAllNodeInfo(const GetAllNodeInfoRequest &request,
                                    GetAllNodeInfoReply *reply,
                                    SendReplyCallback send_reply_callback) = 0;

  virtual void HandleSetInternalConfig(const SetInternalConfigRequest &request,
                                       SetInternalConfigReply *reply,
                                       SendReplyCallback send_reply_callback) = 0;

  virtual void HandleGetInternalConfig(const GetInternalConfigRequest &request,
                                       GetInternalConfigReply *reply,
                                       SendReplyCallback send_reply_callback) = 0;
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
      std::vector<std::unique_ptr<ServerCallFactory>> *server_call_factories) override {
    NODE_INFO_SERVICE_RPC_HANDLER(RegisterNode);
    NODE_INFO_SERVICE_RPC_HANDLER(UnregisterNode);
    NODE_INFO_SERVICE_RPC_HANDLER(GetAllNodeInfo);
    NODE_INFO_SERVICE_RPC_HANDLER(SetInternalConfig);
    NODE_INFO_SERVICE_RPC_HANDLER(GetInternalConfig);
  }

 private:
  /// The grpc async service object.
  NodeInfoGcsService::AsyncService service_;
  /// The service handler that actually handle the requests.
  NodeInfoGcsServiceHandler &service_handler_;
};

class NodeResourceInfoGcsServiceHandler {
 public:
  virtual ~NodeResourceInfoGcsServiceHandler() = default;

  virtual void HandleGetResources(const GetResourcesRequest &request,
                                  GetResourcesReply *reply,
                                  SendReplyCallback send_reply_callback) = 0;

  virtual void HandleUpdateResources(const UpdateResourcesRequest &request,
                                     UpdateResourcesReply *reply,
                                     SendReplyCallback send_reply_callback) = 0;

  virtual void HandleDeleteResources(const DeleteResourcesRequest &request,
                                     DeleteResourcesReply *reply,
                                     SendReplyCallback send_reply_callback) = 0;

  virtual void HandleGetAllAvailableResources(
      const rpc::GetAllAvailableResourcesRequest &request,
      rpc::GetAllAvailableResourcesReply *reply,
      rpc::SendReplyCallback send_reply_callback) = 0;

  virtual void HandleReportResourceUsage(const ReportResourceUsageRequest &request,
                                         ReportResourceUsageReply *reply,
                                         SendReplyCallback send_reply_callback) = 0;

  virtual void HandleGetAllResourceUsage(const GetAllResourceUsageRequest &request,
                                         GetAllResourceUsageReply *reply,
                                         SendReplyCallback send_reply_callback) = 0;
};

/// The `GrpcService` for `NodeResourceInfoGcsService`.
class NodeResourceInfoGrpcService : public GrpcService {
 public:
  /// Constructor.
  ///
  /// \param[in] handler The service handler that actually handle the requests.
  explicit NodeResourceInfoGrpcService(boost::asio::io_service &io_service,
                                       NodeResourceInfoGcsServiceHandler &handler)
      : GrpcService(io_service), service_handler_(handler){};

 protected:
  grpc::Service &GetGrpcService() override { return service_; }

  void InitServerCallFactories(
      const std::unique_ptr<grpc::ServerCompletionQueue> &cq,
      std::vector<std::unique_ptr<ServerCallFactory>> *server_call_factories) override {
    NODE_RESOURCE_INFO_SERVICE_RPC_HANDLER(GetResources);
    NODE_RESOURCE_INFO_SERVICE_RPC_HANDLER(UpdateResources);
    NODE_RESOURCE_INFO_SERVICE_RPC_HANDLER(DeleteResources);
    NODE_RESOURCE_INFO_SERVICE_RPC_HANDLER(GetAllAvailableResources);
    NODE_RESOURCE_INFO_SERVICE_RPC_HANDLER(ReportResourceUsage);
    NODE_RESOURCE_INFO_SERVICE_RPC_HANDLER(GetAllResourceUsage);
  }

 private:
  /// The grpc async service object.
  NodeResourceInfoGcsService::AsyncService service_;
  /// The service handler that actually handle the requests.
  NodeResourceInfoGcsServiceHandler &service_handler_;
};

class HeartbeatInfoGcsServiceHandler {
 public:
  virtual ~HeartbeatInfoGcsServiceHandler() = default;
  virtual void HandleReportHeartbeat(const ReportHeartbeatRequest &request,
                                     ReportHeartbeatReply *reply,
                                     SendReplyCallback send_reply_callback) = 0;
};
/// The `GrpcService` for `HeartbeatInfoGcsService`.
class HeartbeatInfoGrpcService : public GrpcService {
 public:
  /// Constructor.
  ///
  /// \param[in] handler The service handler that actually handle the requests.
  explicit HeartbeatInfoGrpcService(boost::asio::io_service &io_service,
                                    HeartbeatInfoGcsServiceHandler &handler)
      : GrpcService(io_service), service_handler_(handler){};

 protected:
  grpc::Service &GetGrpcService() override { return service_; }
  void InitServerCallFactories(
      const std::unique_ptr<grpc::ServerCompletionQueue> &cq,
      std::vector<std::unique_ptr<ServerCallFactory>> *server_call_factories) override {
    HEARTBEAT_INFO_SERVICE_RPC_HANDLER(ReportHeartbeat);
  }

 private:
  /// The grpc async service object.
  HeartbeatInfoGcsService::AsyncService service_;
  /// The service handler that actually handle the requests.
  HeartbeatInfoGcsServiceHandler &service_handler_;
};

class ObjectInfoGcsServiceHandler {
 public:
  virtual ~ObjectInfoGcsServiceHandler() = default;

  virtual void HandleGetObjectLocations(const GetObjectLocationsRequest &request,
                                        GetObjectLocationsReply *reply,
                                        SendReplyCallback send_reply_callback) = 0;

  virtual void HandleGetAllObjectLocations(const GetAllObjectLocationsRequest &request,
                                           GetAllObjectLocationsReply *reply,
                                           SendReplyCallback send_reply_callback) = 0;

  virtual void HandleAddObjectLocation(const AddObjectLocationRequest &request,
                                       AddObjectLocationReply *reply,
                                       SendReplyCallback send_reply_callback) = 0;

  virtual void HandleRemoveObjectLocation(const RemoveObjectLocationRequest &request,
                                          RemoveObjectLocationReply *reply,
                                          SendReplyCallback send_reply_callback) = 0;
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
      std::vector<std::unique_ptr<ServerCallFactory>> *server_call_factories) override {
    OBJECT_INFO_SERVICE_RPC_HANDLER(GetObjectLocations);
    OBJECT_INFO_SERVICE_RPC_HANDLER(GetAllObjectLocations);
    OBJECT_INFO_SERVICE_RPC_HANDLER(AddObjectLocation);
    OBJECT_INFO_SERVICE_RPC_HANDLER(RemoveObjectLocation);
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

  virtual void HandleAddTask(const AddTaskRequest &request, AddTaskReply *reply,
                             SendReplyCallback send_reply_callback) = 0;

  virtual void HandleGetTask(const GetTaskRequest &request, GetTaskReply *reply,
                             SendReplyCallback send_reply_callback) = 0;

  virtual void HandleAddTaskLease(const AddTaskLeaseRequest &request,
                                  AddTaskLeaseReply *reply,
                                  SendReplyCallback send_reply_callback) = 0;

  virtual void HandleGetTaskLease(const GetTaskLeaseRequest &request,
                                  GetTaskLeaseReply *reply,
                                  SendReplyCallback send_reply_callback) = 0;

  virtual void HandleAttemptTaskReconstruction(
      const AttemptTaskReconstructionRequest &request,
      AttemptTaskReconstructionReply *reply, SendReplyCallback send_reply_callback) = 0;
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
      std::vector<std::unique_ptr<ServerCallFactory>> *server_call_factories) override {
    TASK_INFO_SERVICE_RPC_HANDLER(AddTask);
    TASK_INFO_SERVICE_RPC_HANDLER(GetTask);
    TASK_INFO_SERVICE_RPC_HANDLER(AddTaskLease);
    TASK_INFO_SERVICE_RPC_HANDLER(GetTaskLease);
    TASK_INFO_SERVICE_RPC_HANDLER(AttemptTaskReconstruction);
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

  virtual void HandleAddProfileData(const AddProfileDataRequest &request,
                                    AddProfileDataReply *reply,
                                    SendReplyCallback send_reply_callback) = 0;

  virtual void HandleGetAllProfileInfo(const GetAllProfileInfoRequest &request,
                                       GetAllProfileInfoReply *reply,
                                       SendReplyCallback send_reply_callback) = 0;
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
      std::vector<std::unique_ptr<ServerCallFactory>> *server_call_factories) override {
    STATS_SERVICE_RPC_HANDLER(AddProfileData);
    STATS_SERVICE_RPC_HANDLER(GetAllProfileInfo);
  }

 private:
  /// The grpc async service object.
  StatsGcsService::AsyncService service_;
  /// The service handler that actually handle the requests.
  StatsGcsServiceHandler &service_handler_;
};

class WorkerInfoGcsServiceHandler {
 public:
  virtual ~WorkerInfoGcsServiceHandler() = default;

  virtual void HandleReportWorkerFailure(const ReportWorkerFailureRequest &request,
                                         ReportWorkerFailureReply *reply,
                                         SendReplyCallback send_reply_callback) = 0;

  virtual void HandleGetWorkerInfo(const GetWorkerInfoRequest &request,
                                   GetWorkerInfoReply *reply,
                                   SendReplyCallback send_reply_callback) = 0;

  virtual void HandleGetAllWorkerInfo(const GetAllWorkerInfoRequest &request,
                                      GetAllWorkerInfoReply *reply,
                                      SendReplyCallback send_reply_callback) = 0;

  virtual void HandleAddWorkerInfo(const AddWorkerInfoRequest &request,
                                   AddWorkerInfoReply *reply,
                                   SendReplyCallback send_reply_callback) = 0;
};

/// The `GrpcService` for `WorkerInfoGcsService`.
class WorkerInfoGrpcService : public GrpcService {
 public:
  /// Constructor.
  ///
  /// \param[in] handler The service handler that actually handle the requests.
  explicit WorkerInfoGrpcService(boost::asio::io_service &io_service,
                                 WorkerInfoGcsServiceHandler &handler)
      : GrpcService(io_service), service_handler_(handler){};

 protected:
  grpc::Service &GetGrpcService() override { return service_; }

  void InitServerCallFactories(
      const std::unique_ptr<grpc::ServerCompletionQueue> &cq,
      std::vector<std::unique_ptr<ServerCallFactory>> *server_call_factories) override {
    WORKER_INFO_SERVICE_RPC_HANDLER(ReportWorkerFailure);
    WORKER_INFO_SERVICE_RPC_HANDLER(GetWorkerInfo);
    WORKER_INFO_SERVICE_RPC_HANDLER(GetAllWorkerInfo);
    WORKER_INFO_SERVICE_RPC_HANDLER(AddWorkerInfo);
  }

 private:
  /// The grpc async service object.
  WorkerInfoGcsService::AsyncService service_;
  /// The service handler that actually handle the requests.
  WorkerInfoGcsServiceHandler &service_handler_;
};

class PlacementGroupInfoGcsServiceHandler {
 public:
  virtual ~PlacementGroupInfoGcsServiceHandler() = default;

  virtual void HandleCreatePlacementGroup(const CreatePlacementGroupRequest &request,
                                          CreatePlacementGroupReply *reply,
                                          SendReplyCallback send_reply_callback) = 0;

  virtual void HandleRemovePlacementGroup(const RemovePlacementGroupRequest &request,
                                          RemovePlacementGroupReply *reply,
                                          SendReplyCallback send_reply_callback) = 0;

  virtual void HandleGetPlacementGroup(const GetPlacementGroupRequest &request,
                                       GetPlacementGroupReply *reply,
                                       SendReplyCallback send_reply_callback) = 0;

  virtual void HandleGetAllPlacementGroup(const GetAllPlacementGroupRequest &request,
                                          GetAllPlacementGroupReply *reply,
                                          SendReplyCallback send_reply_callback) = 0;

  virtual void HandleWaitPlacementGroupUntilReady(
      const WaitPlacementGroupUntilReadyRequest &request,
      WaitPlacementGroupUntilReadyReply *reply,
      SendReplyCallback send_reply_callback) = 0;

  virtual void HandleGetNamedPlacementGroup(const GetNamedPlacementGroupRequest &request,
                                            GetNamedPlacementGroupReply *reply,
                                            SendReplyCallback send_reply_callback) = 0;
};

/// The `GrpcService` for `PlacementGroupInfoGcsService`.
class PlacementGroupInfoGrpcService : public GrpcService {
 public:
  /// Constructor.
  ///
  /// \param[in] handler The service handler that actually handle the requests.
  explicit PlacementGroupInfoGrpcService(boost::asio::io_service &io_service,
                                         PlacementGroupInfoGcsServiceHandler &handler)
      : GrpcService(io_service), service_handler_(handler){};

 protected:
  grpc::Service &GetGrpcService() override { return service_; }

  void InitServerCallFactories(
      const std::unique_ptr<grpc::ServerCompletionQueue> &cq,
      std::vector<std::unique_ptr<ServerCallFactory>> *server_call_factories) override {
    PLACEMENT_GROUP_INFO_SERVICE_RPC_HANDLER(CreatePlacementGroup);
    PLACEMENT_GROUP_INFO_SERVICE_RPC_HANDLER(RemovePlacementGroup);
    PLACEMENT_GROUP_INFO_SERVICE_RPC_HANDLER(GetPlacementGroup);
    PLACEMENT_GROUP_INFO_SERVICE_RPC_HANDLER(GetNamedPlacementGroup);
    PLACEMENT_GROUP_INFO_SERVICE_RPC_HANDLER(GetAllPlacementGroup);
    PLACEMENT_GROUP_INFO_SERVICE_RPC_HANDLER(WaitPlacementGroupUntilReady);
  }

 private:
  /// The grpc async service object.
  PlacementGroupInfoGcsService::AsyncService service_;
  /// The service handler that actually handle the requests.
  PlacementGroupInfoGcsServiceHandler &service_handler_;
};

using JobInfoHandler = JobInfoGcsServiceHandler;
using ActorInfoHandler = ActorInfoGcsServiceHandler;
using NodeInfoHandler = NodeInfoGcsServiceHandler;
using NodeResourceInfoHandler = NodeResourceInfoGcsServiceHandler;
using HeartbeatInfoHandler = HeartbeatInfoGcsServiceHandler;
using ObjectInfoHandler = ObjectInfoGcsServiceHandler;
using TaskInfoHandler = TaskInfoGcsServiceHandler;
using StatsHandler = StatsGcsServiceHandler;
using WorkerInfoHandler = WorkerInfoGcsServiceHandler;
using PlacementGroupInfoHandler = PlacementGroupInfoGcsServiceHandler;

}  // namespace rpc
}  // namespace ray
