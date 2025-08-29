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

#include <memory>
#include <vector>

#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/id.h"
#include "ray/rpc/grpc_server.h"
#include "ray/rpc/server_call.h"
#include "src/ray/protobuf/autoscaler.grpc.pb.h"
#include "src/ray/protobuf/events_event_aggregator_service.pb.h"
#include "src/ray/protobuf/gcs_service.grpc.pb.h"

namespace ray {
namespace rpc {
// Most of our RPC templates, if not all, expect messages in the ray::rpc protobuf
// namespace.  Since the following two messages are defined under the rpc::events
// namespace, we treat them as if they were part of ray::rpc for compatibility.
using ray::rpc::events::AddEventsReply;
using ray::rpc::events::AddEventsRequest;
namespace autoscaler {

#define AUTOSCALER_STATE_SERVICE_RPC_HANDLER(HANDLER) \
  RPC_SERVICE_HANDLER(AutoscalerStateService,         \
                      HANDLER,                        \
                      RayConfig::instance().gcs_max_active_rpcs_per_handler())

class AutoscalerStateServiceHandler {
 public:
  virtual ~AutoscalerStateServiceHandler() = default;

  virtual void HandleGetClusterResourceState(GetClusterResourceStateRequest request,
                                             GetClusterResourceStateReply *reply,
                                             SendReplyCallback send_reply_callback) = 0;

  virtual void HandleReportAutoscalingState(ReportAutoscalingStateRequest request,
                                            ReportAutoscalingStateReply *reply,
                                            SendReplyCallback send_reply_callback) = 0;

  virtual void HandleRequestClusterResourceConstraint(
      RequestClusterResourceConstraintRequest request,
      RequestClusterResourceConstraintReply *reply,
      SendReplyCallback send_reply_callback) = 0;

  virtual void HandleGetClusterStatus(GetClusterStatusRequest request,
                                      GetClusterStatusReply *reply,
                                      SendReplyCallback send_reply_callback) = 0;

  virtual void HandleDrainNode(DrainNodeRequest request,
                               DrainNodeReply *reply,
                               SendReplyCallback send_reply_callback) = 0;

  virtual void HandleReportClusterConfig(ReportClusterConfigRequest request,
                                         ReportClusterConfigReply *reply,
                                         SendReplyCallback send_reply_callback) = 0;
};

/// The `GrpcService` for `AutoscalerStateService`.
class AutoscalerStateGrpcService : public GrpcService {
 public:
  /// Constructor.
  ///
  /// \param[in] handler The service handler that actually handle the requests.
  explicit AutoscalerStateGrpcService(instrumented_io_context &io_service,
                                      AutoscalerStateServiceHandler &handler)
      : GrpcService(io_service), service_handler_(handler){};

 protected:
  grpc::Service &GetGrpcService() override { return service_; }
  void InitServerCallFactories(
      const std::unique_ptr<grpc::ServerCompletionQueue> &cq,
      std::vector<std::unique_ptr<ServerCallFactory>> *server_call_factories,
      const ClusterID &cluster_id) override {
    AUTOSCALER_STATE_SERVICE_RPC_HANDLER(GetClusterResourceState);
    AUTOSCALER_STATE_SERVICE_RPC_HANDLER(ReportAutoscalingState);
    AUTOSCALER_STATE_SERVICE_RPC_HANDLER(ReportClusterConfig);
    AUTOSCALER_STATE_SERVICE_RPC_HANDLER(RequestClusterResourceConstraint);
    AUTOSCALER_STATE_SERVICE_RPC_HANDLER(GetClusterStatus);
    AUTOSCALER_STATE_SERVICE_RPC_HANDLER(DrainNode);
  }

 private:
  /// The grpc async service object.
  AutoscalerStateService::AsyncService service_;
  /// The service handler that actually handle the requests.
  AutoscalerStateServiceHandler &service_handler_;
};

using AutoscalerStateHandler = AutoscalerStateServiceHandler;

}  // namespace autoscaler
}  // namespace rpc
}  // namespace ray

namespace ray {
namespace rpc {

#define ACTOR_INFO_SERVICE_RPC_HANDLER(HANDLER, MAX_ACTIVE_RPCS) \
  RPC_SERVICE_HANDLER(ActorInfoGcsService, HANDLER, MAX_ACTIVE_RPCS)

#define MONITOR_SERVICE_RPC_HANDLER(HANDLER) \
  RPC_SERVICE_HANDLER(MonitorGcsService,     \
                      HANDLER,               \
                      RayConfig::instance().gcs_max_active_rpcs_per_handler())

#define OBJECT_INFO_SERVICE_RPC_HANDLER(HANDLER) \
  RPC_SERVICE_HANDLER(ObjectInfoGcsService,      \
                      HANDLER,                   \
                      RayConfig::instance().gcs_max_active_rpcs_per_handler())

#define GCS_RPC_SEND_REPLY(send_reply_callback, reply, status)        \
  reply->mutable_status()->set_code(static_cast<int>(status.code())); \
  reply->mutable_status()->set_message(status.message());             \
  send_reply_callback(ray::Status::OK(), nullptr, nullptr)

class ActorInfoGcsServiceHandler {
 public:
  virtual ~ActorInfoGcsServiceHandler() = default;

  virtual void HandleRegisterActor(RegisterActorRequest request,
                                   RegisterActorReply *reply,
                                   SendReplyCallback send_reply_callback) = 0;

  virtual void HandleRestartActorForLineageReconstruction(
      RestartActorForLineageReconstructionRequest request,
      RestartActorForLineageReconstructionReply *reply,
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

  virtual void HandleReportActorOutOfScope(ReportActorOutOfScopeRequest request,
                                           ReportActorOutOfScopeReply *reply,
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
      std::vector<std::unique_ptr<ServerCallFactory>> *server_call_factories,
      const ClusterID &cluster_id) override {
    /// Register/Create Actor RPC takes long time, we shouldn't limit them to avoid
    /// distributed deadlock.
    ACTOR_INFO_SERVICE_RPC_HANDLER(RegisterActor, -1);
    ACTOR_INFO_SERVICE_RPC_HANDLER(RestartActorForLineageReconstruction, -1);
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
    ACTOR_INFO_SERVICE_RPC_HANDLER(
        ReportActorOutOfScope, RayConfig::instance().gcs_max_active_rpcs_per_handler());
  }

 private:
  /// The grpc async service object.
  ActorInfoGcsService::AsyncService service_;
  /// The service handler that actually handle the requests.
  ActorInfoGcsServiceHandler &service_handler_;
};

using ActorInfoHandler = ActorInfoGcsServiceHandler;

}  // namespace rpc
}  // namespace ray
