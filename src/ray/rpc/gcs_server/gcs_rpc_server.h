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

#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/id.h"
#include "ray/rpc/grpc_server.h"
#include "ray/rpc/server_call.h"
#include "src/ray/protobuf/gcs_service.grpc.pb.h"
#include "src/ray/protobuf/monitor.grpc.pb.h"

namespace ray {
namespace rpc {

#define JOB_INFO_SERVICE_RPC_HANDLER(HANDLER) \
  RPC_SERVICE_HANDLER(JobInfoGcsService,      \
                      HANDLER,                \
                      RayConfig::instance().gcs_max_active_rpcs_per_handler())

#define ACTOR_INFO_SERVICE_RPC_HANDLER(HANDLER, MAX_ACTIVE_RPCS) \
  RPC_SERVICE_HANDLER(ActorInfoGcsService, HANDLER, MAX_ACTIVE_RPCS)

#define MONITOR_SERVICE_RPC_HANDLER(HANDLER) \
  RPC_SERVICE_HANDLER(MonitorGrpcService,    \
                      HANDLER,               \
                      RayConfig::instance().gcs_max_active_rpcs_per_handler())

#define NODE_INFO_SERVICE_RPC_HANDLER(HANDLER) \
  RPC_SERVICE_HANDLER(NodeInfoGcsService,      \
                      HANDLER,                 \
                      RayConfig::instance().gcs_max_active_rpcs_per_handler())

#define TASK_INFO_SERVICE_RPC_HANDLER(HANDLER) \
  RPC_SERVICE_HANDLER(TaskInfoGcsService,      \
                      HANDLER,                 \
                      RayConfig::instance().gcs_max_active_rpcs_per_handler())

#define NODE_RESOURCE_INFO_SERVICE_RPC_HANDLER(HANDLER) \
  RPC_SERVICE_HANDLER(NodeResourceInfoGcsService,       \
                      HANDLER,                          \
                      RayConfig::instance().gcs_max_active_rpcs_per_handler())

#define OBJECT_INFO_SERVICE_RPC_HANDLER(HANDLER) \
  RPC_SERVICE_HANDLER(ObjectInfoGcsService,      \
                      HANDLER,                   \
                      RayConfig::instance().gcs_max_active_rpcs_per_handler())

#define WORKER_INFO_SERVICE_RPC_HANDLER(HANDLER) \
  RPC_SERVICE_HANDLER(WorkerInfoGcsService,      \
                      HANDLER,                   \
                      RayConfig::instance().gcs_max_active_rpcs_per_handler())

#define PLACEMENT_GROUP_INFO_SERVICE_RPC_HANDLER(HANDLER) \
  RPC_SERVICE_HANDLER(PlacementGroupInfoGcsService,       \
                      HANDLER,                            \
                      RayConfig::instance().gcs_max_active_rpcs_per_handler())

#define INTERNAL_KV_SERVICE_RPC_HANDLER(HANDLER) \
  RPC_SERVICE_HANDLER(InternalKVGcsService, HANDLER, -1)

#define RUNTIME_ENV_SERVICE_RPC_HANDLER(HANDLER) \
  RPC_SERVICE_HANDLER(RuntimeEnvGcsService, HANDLER, -1)

// Unlimited max active RPCs, because of long poll.
#define INTERNAL_PUBSUB_SERVICE_RPC_HANDLER(HANDLER) \
  RPC_SERVICE_HANDLER(InternalPubSubGcsService, HANDLER, -1)

#define GCS_RPC_SEND_REPLY(send_reply_callback, reply, status) \
  reply->mutable_status()->set_code((int)status.code());       \
  reply->mutable_status()->set_message(status.message());      \
  send_reply_callback(ray::Status::OK(), nullptr, nullptr)

class JobInfoGcsServiceHandler {
 public:
  virtual ~JobInfoGcsServiceHandler() = default;

  virtual void HandleAddJob(AddJobRequest request,
                            AddJobReply *reply,
                            SendReplyCallback send_reply_callback) = 0;

  virtual void HandleMarkJobFinished(MarkJobFinishedRequest request,
                                     MarkJobFinishedReply *reply,
                                     SendReplyCallback send_reply_callback) = 0;

  virtual void HandleGetAllJobInfo(GetAllJobInfoRequest request,
                                   GetAllJobInfoReply *reply,
                                   SendReplyCallback send_reply_callback) = 0;

  virtual void AddJobFinishedListener(
      std::function<void(std::shared_ptr<JobID>)> listener) = 0;

  virtual void HandleReportJobError(ReportJobErrorRequest request,
                                    ReportJobErrorReply *reply,
                                    SendReplyCallback send_reply_callback) = 0;

  virtual void HandleGetNextJobID(GetNextJobIDRequest request,
                                  GetNextJobIDReply *reply,
                                  SendReplyCallback send_reply_callback) = 0;
};

/// The `GrpcService` for `JobInfoGcsService`.
class JobInfoGrpcService : public GrpcService {
 public:
  /// Constructor.
  ///
  /// \param[in] handler The service handler that actually handle the requests.
  explicit JobInfoGrpcService(instrumented_io_context &io_service,
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
    JOB_INFO_SERVICE_RPC_HANDLER(ReportJobError);
    JOB_INFO_SERVICE_RPC_HANDLER(GetNextJobID);
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

  virtual void HandleRegisterActor(RegisterActorRequest request,
                                   RegisterActorReply *reply,
                                   SendReplyCallback send_reply_callback) = 0;

  virtual void HandleCreateActor(CreateActorRequest request,
                                 CreateActorReply *reply,
                                 SendReplyCallback send_reply_callback) = 0;

  virtual void HandleGetActorInfo(GetActorInfoRequest request,
                                  GetActorInfoReply *reply,
                                  SendReplyCallback send_reply_callback) = 0;

  virtual void HandleGetNamedActorInfo(GetNamedActorInfoRequest request,
                                       GetNamedActorInfoReply *reply,
                                       SendReplyCallback send_reply_callback) = 0;

  virtual void HandleListNamedActors(rpc::ListNamedActorsRequest request,
                                     rpc::ListNamedActorsReply *reply,
                                     rpc::SendReplyCallback send_reply_callback) = 0;

  virtual void HandleGetAllActorInfo(GetAllActorInfoRequest request,
                                     GetAllActorInfoReply *reply,
                                     SendReplyCallback send_reply_callback) = 0;

  virtual void HandleKillActorViaGcs(KillActorViaGcsRequest request,
                                     KillActorViaGcsReply *reply,
                                     SendReplyCallback send_reply_callback) = 0;
};

/// The `GrpcService` for `ActorInfoGcsService`.
class ActorInfoGrpcService : public GrpcService {
 public:
  /// Constructor.
  ///
  /// \param[in] handler The service handler that actually handle the requests.
  explicit ActorInfoGrpcService(instrumented_io_context &io_service,
                                ActorInfoGcsServiceHandler &handler)
      : GrpcService(io_service), service_handler_(handler){};

 protected:
  grpc::Service &GetGrpcService() override { return service_; }

  void InitServerCallFactories(
      const std::unique_ptr<grpc::ServerCompletionQueue> &cq,
      std::vector<std::unique_ptr<ServerCallFactory>> *server_call_factories) override {
    /// Register/Create Actor RPC takes long time, we shouldn't limit them to avoid
    /// distributed deadlock.
    ACTOR_INFO_SERVICE_RPC_HANDLER(RegisterActor, -1);
    ACTOR_INFO_SERVICE_RPC_HANDLER(CreateActor, -1);

    /// Others need back pressure.
    ACTOR_INFO_SERVICE_RPC_HANDLER(
        GetActorInfo, RayConfig::instance().gcs_max_active_rpcs_per_handler());
    ACTOR_INFO_SERVICE_RPC_HANDLER(
        GetNamedActorInfo, RayConfig::instance().gcs_max_active_rpcs_per_handler());
    ACTOR_INFO_SERVICE_RPC_HANDLER(
        ListNamedActors, RayConfig::instance().gcs_max_active_rpcs_per_handler());
    ACTOR_INFO_SERVICE_RPC_HANDLER(
        GetAllActorInfo, RayConfig::instance().gcs_max_active_rpcs_per_handler());
    ACTOR_INFO_SERVICE_RPC_HANDLER(
        KillActorViaGcs, RayConfig::instance().gcs_max_active_rpcs_per_handler());
  }

 private:
  /// The grpc async service object.
  ActorInfoGcsService::AsyncService service_;
  /// The service handler that actually handle the requests.
  ActorInfoGcsServiceHandler &service_handler_;
};

class MonitorGrpcServiceHandler {
 public:
  virtual ~MonitorGrpcServiceHandler() = default;

  virtual void HandleGetRayVersion(GetRayVersionRequest request,
                                   GetRayVersionReply *reply,
                                   SendReplyCallback send_reply_callback) = 0;
};

/// The `GrpcService` for `MonitorService`.
class MonitorGrpcService : public GrpcService {
 public:
  /// Constructor.
  ///
  /// \param[in] handler The service handler that actually handle the requests.
  explicit MonitorGrpcService(instrumented_io_context &io_service,
                              MonitorServiceHandler &handler)
      : GrpcService(io_service), service_handler_(handler){};

 protected:
  grpc::Service &GetGrpcService() override { return service_; }

  void InitServerCallFactories(
      const std::unique_ptr<grpc::ServerCompletionQueue> &cq,
      std::vector<std::unique_ptr<ServerCallFactory>> *server_call_factories) override {
    MONITOR_SERVICE_RPC_HANDLER(GetRayVersion);
  }

 private:
  /// The grpc async service object.
  MonitorService::AsyncService service_;
  /// The service handler that actually handle the requests.
  MonitorServiceHandler &service_handler_;
};

class NodeInfoGcsServiceHandler {
 public:
  virtual ~NodeInfoGcsServiceHandler() = default;

  virtual void HandleRegisterNode(RegisterNodeRequest request,
                                  RegisterNodeReply *reply,
                                  SendReplyCallback send_reply_callback) = 0;

  virtual void HandleCheckAlive(CheckAliveRequest request,
                                CheckAliveReply *reply,
                                SendReplyCallback send_reply_callback) = 0;

  virtual void HandleDrainNode(DrainNodeRequest request,
                               DrainNodeReply *reply,
                               SendReplyCallback send_reply_callback) = 0;

  virtual void HandleGetAllNodeInfo(GetAllNodeInfoRequest request,
                                    GetAllNodeInfoReply *reply,
                                    SendReplyCallback send_reply_callback) = 0;

  virtual void HandleGetInternalConfig(GetInternalConfigRequest request,
                                       GetInternalConfigReply *reply,
                                       SendReplyCallback send_reply_callback) = 0;
};

/// The `GrpcService` for `NodeInfoGcsService`.
class NodeInfoGrpcService : public GrpcService {
 public:
  /// Constructor.
  ///
  /// \param[in] handler The service handler that actually handle the requests.
  explicit NodeInfoGrpcService(instrumented_io_context &io_service,
                               NodeInfoGcsServiceHandler &handler)
      : GrpcService(io_service), service_handler_(handler){};

 protected:
  grpc::Service &GetGrpcService() override { return service_; }

  void InitServerCallFactories(
      const std::unique_ptr<grpc::ServerCompletionQueue> &cq,
      std::vector<std::unique_ptr<ServerCallFactory>> *server_call_factories) override {
    NODE_INFO_SERVICE_RPC_HANDLER(RegisterNode);
    NODE_INFO_SERVICE_RPC_HANDLER(DrainNode);
    NODE_INFO_SERVICE_RPC_HANDLER(GetAllNodeInfo);
    NODE_INFO_SERVICE_RPC_HANDLER(GetInternalConfig);
    NODE_INFO_SERVICE_RPC_HANDLER(CheckAlive);
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

  virtual void HandleGetResources(GetResourcesRequest request,
                                  GetResourcesReply *reply,
                                  SendReplyCallback send_reply_callback) = 0;

  virtual void HandleGetAllAvailableResources(
      rpc::GetAllAvailableResourcesRequest request,
      rpc::GetAllAvailableResourcesReply *reply,
      rpc::SendReplyCallback send_reply_callback) = 0;

  virtual void HandleReportResourceUsage(ReportResourceUsageRequest request,
                                         ReportResourceUsageReply *reply,
                                         SendReplyCallback send_reply_callback) = 0;

  virtual void HandleGetAllResourceUsage(GetAllResourceUsageRequest request,
                                         GetAllResourceUsageReply *reply,
                                         SendReplyCallback send_reply_callback) = 0;
};

/// The `GrpcService` for `NodeResourceInfoGcsService`.
class NodeResourceInfoGrpcService : public GrpcService {
 public:
  /// Constructor.
  ///
  /// \param[in] handler The service handler that actually handle the requests.
  explicit NodeResourceInfoGrpcService(instrumented_io_context &io_service,
                                       NodeResourceInfoGcsServiceHandler &handler)
      : GrpcService(io_service), service_handler_(handler){};

 protected:
  grpc::Service &GetGrpcService() override { return service_; }

  void InitServerCallFactories(
      const std::unique_ptr<grpc::ServerCompletionQueue> &cq,
      std::vector<std::unique_ptr<ServerCallFactory>> *server_call_factories) override {
    NODE_RESOURCE_INFO_SERVICE_RPC_HANDLER(GetResources);
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

class WorkerInfoGcsServiceHandler {
 public:
  virtual ~WorkerInfoGcsServiceHandler() = default;

  virtual void HandleReportWorkerFailure(ReportWorkerFailureRequest request,
                                         ReportWorkerFailureReply *reply,
                                         SendReplyCallback send_reply_callback) = 0;

  virtual void HandleGetWorkerInfo(GetWorkerInfoRequest request,
                                   GetWorkerInfoReply *reply,
                                   SendReplyCallback send_reply_callback) = 0;

  virtual void HandleGetAllWorkerInfo(GetAllWorkerInfoRequest request,
                                      GetAllWorkerInfoReply *reply,
                                      SendReplyCallback send_reply_callback) = 0;

  virtual void HandleAddWorkerInfo(AddWorkerInfoRequest request,
                                   AddWorkerInfoReply *reply,
                                   SendReplyCallback send_reply_callback) = 0;
};

/// The `GrpcService` for `WorkerInfoGcsService`.
class WorkerInfoGrpcService : public GrpcService {
 public:
  /// Constructor.
  ///
  /// \param[in] handler The service handler that actually handle the requests.
  explicit WorkerInfoGrpcService(instrumented_io_context &io_service,
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

  virtual void HandleCreatePlacementGroup(CreatePlacementGroupRequest request,
                                          CreatePlacementGroupReply *reply,
                                          SendReplyCallback send_reply_callback) = 0;

  virtual void HandleRemovePlacementGroup(RemovePlacementGroupRequest request,
                                          RemovePlacementGroupReply *reply,
                                          SendReplyCallback send_reply_callback) = 0;

  virtual void HandleGetPlacementGroup(GetPlacementGroupRequest request,
                                       GetPlacementGroupReply *reply,
                                       SendReplyCallback send_reply_callback) = 0;

  virtual void HandleGetAllPlacementGroup(GetAllPlacementGroupRequest request,
                                          GetAllPlacementGroupReply *reply,
                                          SendReplyCallback send_reply_callback) = 0;

  virtual void HandleWaitPlacementGroupUntilReady(
      WaitPlacementGroupUntilReadyRequest request,
      WaitPlacementGroupUntilReadyReply *reply,
      SendReplyCallback send_reply_callback) = 0;

  virtual void HandleGetNamedPlacementGroup(GetNamedPlacementGroupRequest request,
                                            GetNamedPlacementGroupReply *reply,
                                            SendReplyCallback send_reply_callback) = 0;
};

/// The `GrpcService` for `PlacementGroupInfoGcsService`.
class PlacementGroupInfoGrpcService : public GrpcService {
 public:
  /// Constructor.
  ///
  /// \param[in] handler The service handler that actually handle the requests.
  explicit PlacementGroupInfoGrpcService(instrumented_io_context &io_service,
                                         PlacementGroupInfoGcsServiceHandler &handler)
      : GrpcService(io_service), service_handler_(handler) {}

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

class InternalKVGcsServiceHandler {
 public:
  virtual ~InternalKVGcsServiceHandler() = default;
  virtual void HandleInternalKVKeys(InternalKVKeysRequest request,
                                    InternalKVKeysReply *reply,
                                    SendReplyCallback send_reply_callback) = 0;

  virtual void HandleInternalKVGet(InternalKVGetRequest request,
                                   InternalKVGetReply *reply,
                                   SendReplyCallback send_reply_callback) = 0;

  virtual void HandleInternalKVPut(InternalKVPutRequest request,
                                   InternalKVPutReply *reply,
                                   SendReplyCallback send_reply_callback) = 0;

  virtual void HandleInternalKVDel(InternalKVDelRequest request,
                                   InternalKVDelReply *reply,
                                   SendReplyCallback send_reply_callback) = 0;

  virtual void HandleInternalKVExists(InternalKVExistsRequest request,
                                      InternalKVExistsReply *reply,
                                      SendReplyCallback send_reply_callback) = 0;
};

class InternalKVGrpcService : public GrpcService {
 public:
  explicit InternalKVGrpcService(instrumented_io_context &io_service,
                                 InternalKVGcsServiceHandler &handler)
      : GrpcService(io_service), service_handler_(handler) {}

 protected:
  grpc::Service &GetGrpcService() override { return service_; }
  void InitServerCallFactories(
      const std::unique_ptr<grpc::ServerCompletionQueue> &cq,
      std::vector<std::unique_ptr<ServerCallFactory>> *server_call_factories) override {
    INTERNAL_KV_SERVICE_RPC_HANDLER(InternalKVGet);
    INTERNAL_KV_SERVICE_RPC_HANDLER(InternalKVPut);
    INTERNAL_KV_SERVICE_RPC_HANDLER(InternalKVDel);
    INTERNAL_KV_SERVICE_RPC_HANDLER(InternalKVExists);
    INTERNAL_KV_SERVICE_RPC_HANDLER(InternalKVKeys);
  }

 private:
  InternalKVGcsService::AsyncService service_;
  InternalKVGcsServiceHandler &service_handler_;
};

class RuntimeEnvGcsServiceHandler {
 public:
  virtual ~RuntimeEnvGcsServiceHandler() = default;
  virtual void HandlePinRuntimeEnvURI(PinRuntimeEnvURIRequest request,
                                      PinRuntimeEnvURIReply *reply,
                                      SendReplyCallback send_reply_callback) = 0;
};

class RuntimeEnvGrpcService : public GrpcService {
 public:
  explicit RuntimeEnvGrpcService(instrumented_io_context &io_service,
                                 RuntimeEnvGcsServiceHandler &handler)
      : GrpcService(io_service), service_handler_(handler) {}

 protected:
  grpc::Service &GetGrpcService() override { return service_; }
  void InitServerCallFactories(
      const std::unique_ptr<grpc::ServerCompletionQueue> &cq,
      std::vector<std::unique_ptr<ServerCallFactory>> *server_call_factories) override {
    RUNTIME_ENV_SERVICE_RPC_HANDLER(PinRuntimeEnvURI);
  }

 private:
  RuntimeEnvGcsService::AsyncService service_;
  RuntimeEnvGcsServiceHandler &service_handler_;
};

class TaskInfoGcsServiceHandler {
 public:
  virtual ~TaskInfoGcsServiceHandler() = default;

  virtual void HandleAddTaskEventData(AddTaskEventDataRequest request,
                                      AddTaskEventDataReply *reply,
                                      SendReplyCallback send_reply_callback) = 0;

  virtual void HandleGetTaskEvents(rpc::GetTaskEventsRequest request,
                                   rpc::GetTaskEventsReply *reply,
                                   rpc::SendReplyCallback send_reply_callback) = 0;
};

/// The `GrpcService` for `TaskInfoGcsService`.
class TaskInfoGrpcService : public GrpcService {
 public:
  /// Constructor.
  ///
  /// \param[in] io_service IO service to run the handler.
  /// \param[in] handler The service handler that actually handle the requests.
  explicit TaskInfoGrpcService(instrumented_io_context &io_service,
                               TaskInfoGcsServiceHandler &handler)
      : GrpcService(io_service), service_handler_(handler){};

 protected:
  grpc::Service &GetGrpcService() override { return service_; }

  void InitServerCallFactories(
      const std::unique_ptr<grpc::ServerCompletionQueue> &cq,
      std::vector<std::unique_ptr<ServerCallFactory>> *server_call_factories) override {
    TASK_INFO_SERVICE_RPC_HANDLER(AddTaskEventData);
    TASK_INFO_SERVICE_RPC_HANDLER(GetTaskEvents);
  }

 private:
  /// The grpc async service object.
  TaskInfoGcsService::AsyncService service_;
  /// The service handler that actually handle the requests.
  TaskInfoGcsServiceHandler &service_handler_;
};

class InternalPubSubGcsServiceHandler {
 public:
  virtual ~InternalPubSubGcsServiceHandler() = default;

  virtual void HandleGcsPublish(GcsPublishRequest request,
                                GcsPublishReply *reply,
                                SendReplyCallback send_reply_callback) = 0;

  virtual void HandleGcsSubscriberPoll(GcsSubscriberPollRequest request,
                                       GcsSubscriberPollReply *reply,
                                       SendReplyCallback send_reply_callback) = 0;

  virtual void HandleGcsSubscriberCommandBatch(GcsSubscriberCommandBatchRequest request,
                                               GcsSubscriberCommandBatchReply *reply,
                                               SendReplyCallback send_reply_callback) = 0;
};

class InternalPubSubGrpcService : public GrpcService {
 public:
  InternalPubSubGrpcService(instrumented_io_context &io_service,
                            InternalPubSubGcsServiceHandler &handler)
      : GrpcService(io_service), service_handler_(handler) {}

 protected:
  grpc::Service &GetGrpcService() override { return service_; }
  void InitServerCallFactories(
      const std::unique_ptr<grpc::ServerCompletionQueue> &cq,
      std::vector<std::unique_ptr<ServerCallFactory>> *server_call_factories) override {
    INTERNAL_PUBSUB_SERVICE_RPC_HANDLER(GcsPublish);
    INTERNAL_PUBSUB_SERVICE_RPC_HANDLER(GcsSubscriberPoll);
    INTERNAL_PUBSUB_SERVICE_RPC_HANDLER(GcsSubscriberCommandBatch);
  }

 private:
  InternalPubSubGcsService::AsyncService service_;
  InternalPubSubGcsServiceHandler &service_handler_;
};

using JobInfoHandler = JobInfoGcsServiceHandler;
using ActorInfoHandler = ActorInfoGcsServiceHandler;
using MonitorServiceHandler = MonitorGrpcServiceHandler;
using NodeInfoHandler = NodeInfoGcsServiceHandler;
using NodeResourceInfoHandler = NodeResourceInfoGcsServiceHandler;
using WorkerInfoHandler = WorkerInfoGcsServiceHandler;
using PlacementGroupInfoHandler = PlacementGroupInfoGcsServiceHandler;
using InternalKVHandler = InternalKVGcsServiceHandler;
using InternalPubSubHandler = InternalPubSubGcsServiceHandler;
using RuntimeEnvHandler = RuntimeEnvGcsServiceHandler;
using TaskInfoHandler = TaskInfoGcsServiceHandler;

}  // namespace rpc
}  // namespace ray
