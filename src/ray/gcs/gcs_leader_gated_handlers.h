// Copyright 2025 The Ray Authors.
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

#include <functional>
#include <memory>
#include <utility>

#include "ray/common/status.h"
#include "ray/gcs/grpc_service_interfaces.h"

namespace ray {
namespace gcs {

// Helper macro to send reply status. Wrapped in do/while(0) so the multi-statement
// body behaves as a single statement in any context (e.g. an unbraced if/else).
#define GCS_PROXY_SEND_REPLY(send_reply_callback, reply, status)        \
  do {                                                                  \
    reply->mutable_status()->set_code(static_cast<int>(status.code())); \
    reply->mutable_status()->set_message(status.message());             \
    send_reply_callback(ray::Status::OK(), nullptr, nullptr);           \
  } while (0)

// Macros to define a leader-gated proxy handler override in one line each.
// Request/Reply must be fully-qualified (e.g. rpc::AddJobRequest). Suffixes:
//   *_GATED   -> blocked on passive GCS (returns Status::GcsPassive()).
//   *_ALLOWED -> always forwarded (bootstrap allowlist).
//   *_PEER    -> variant taking `const std::string &grpc_peer`.
//   *_CB      -> gate status is sent via the callback, not the reply body.

// Gate reply written into the reply body (default GCS convention).
#define GCS_GATED_RPC(Method, Request, Reply)                                            \
  void Method(Request request, Reply *reply, rpc::SendReplyCallback send_reply_callback) \
      override {                                                                         \
    if (!is_leader_fn_()) {                                                              \
      GCS_PROXY_SEND_REPLY(send_reply_callback, reply, Status::GcsPassive());            \
      return;                                                                            \
    }                                                                                    \
    handler_.Method(std::move(request), reply, std::move(send_reply_callback));          \
  }

#define GCS_GATED_RPC_PEER(Method, Request, Reply)                             \
  void Method(Request request,                                                 \
              Reply *reply,                                                    \
              rpc::SendReplyCallback send_reply_callback,                      \
              const std::string &grpc_peer) override {                         \
    if (!is_leader_fn_()) {                                                    \
      GCS_PROXY_SEND_REPLY(send_reply_callback, reply, Status::GcsPassive());  \
      return;                                                                  \
    }                                                                          \
    handler_.Method(                                                           \
        std::move(request), reply, std::move(send_reply_callback), grpc_peer); \
  }

#define GCS_ALLOWED_RPC(Method, Request, Reply)                                          \
  void Method(Request request, Reply *reply, rpc::SendReplyCallback send_reply_callback) \
      override {                                                                         \
    handler_.Method(std::move(request), reply, std::move(send_reply_callback));          \
  }

// Gate reply written into the callback status.
#define GCS_GATED_RPC_CB(Method, Request, Reply)                                         \
  void Method(Request request, Reply *reply, rpc::SendReplyCallback send_reply_callback) \
      override {                                                                         \
    if (!is_leader_fn_()) {                                                              \
      send_reply_callback(Status::GcsPassive(), nullptr, nullptr);                       \
      return;                                                                            \
    }                                                                                    \
    handler_.Method(std::move(request), reply, std::move(send_reply_callback));          \
  }

#define GCS_GATED_RPC_CB_PEER(Method, Request, Reply)                          \
  void Method(Request request,                                                 \
              Reply *reply,                                                    \
              rpc::SendReplyCallback send_reply_callback,                      \
              const std::string &grpc_peer) override {                         \
    if (!is_leader_fn_()) {                                                    \
      send_reply_callback(Status::GcsPassive(), nullptr, nullptr);             \
      return;                                                                  \
    }                                                                          \
    handler_.Method(                                                           \
        std::move(request), reply, std::move(send_reply_callback), grpc_peer); \
  }

class LeaderGatedNodeInfoHandler : public rpc::NodeInfoGcsServiceHandler {
 public:
  using HandlerType = rpc::NodeInfoGcsServiceHandler;
  LeaderGatedNodeInfoHandler(
      rpc::NodeInfoGcsServiceHandler &handler,
      std::function<bool()> is_leader_fn,
      std::function<void(const rpc::GcsNodeInfo &)> cache_local_node_fn)
      : handler_(handler),
        is_leader_fn_(std::move(is_leader_fn)),
        cache_local_node_fn_(std::move(cache_local_node_fn)) {}

  // Gated on passive GCS.

  GCS_GATED_RPC_PEER(HandleUnregisterNode,
                     rpc::UnregisterNodeRequest,
                     rpc::UnregisterNodeReply)
  GCS_GATED_RPC(HandleDrainNode, rpc::DrainNodeRequest, rpc::DrainNodeReply)

  // Allowed on passive GCS (bootstrap).

  GCS_ALLOWED_RPC(HandleGetClusterId, rpc::GetClusterIdRequest, rpc::GetClusterIdReply)
  GCS_ALLOWED_RPC(HandleCheckAlive, rpc::CheckAliveRequest, rpc::CheckAliveReply)
  GCS_ALLOWED_RPC(HandleGetAllNodeInfo,
                  rpc::GetAllNodeInfoRequest,
                  rpc::GetAllNodeInfoReply)
  GCS_ALLOWED_RPC(HandleGetAllNodeAddressAndLiveness,
                  rpc::GetAllNodeAddressAndLivenessRequest,
                  rpc::GetAllNodeAddressAndLivenessReply)

  void HandleRegisterNode(rpc::RegisterNodeRequest request,
                          rpc::RegisterNodeReply *reply,
                          rpc::SendReplyCallback send_reply_callback) override {
    // Passive GCS blocks remote worker node registrations, but allows the colocated local
    // head node Raylet to register so that dashboard/health check services can start.
    if (!is_leader_fn_()) {
      const rpc::GcsNodeInfo &node_info = request.node_info();
      if (!node_info.is_head_node()) {
        // Reject remote worker node registrations on passive GCS
        GCS_PROXY_SEND_REPLY(send_reply_callback, reply, Status::GcsPassive());
        return;
      }
      // Cache the local head node in-memory without persisting to Redis
      cache_local_node_fn_(node_info);
      GCS_PROXY_SEND_REPLY(send_reply_callback, reply, Status::OK());
      return;
    }
    // Forward to the underlying handler on active GCS
    handler_.HandleRegisterNode(
        std::move(request), reply, std::move(send_reply_callback));
  }

 private:
  rpc::NodeInfoGcsServiceHandler &handler_;
  const std::function<bool()> is_leader_fn_;
  const std::function<void(const rpc::GcsNodeInfo &)> cache_local_node_fn_;
};

class LeaderGatedActorInfoHandler : public rpc::ActorInfoGcsServiceHandler {
 public:
  using HandlerType = rpc::ActorInfoGcsServiceHandler;
  LeaderGatedActorInfoHandler(rpc::ActorInfoGcsServiceHandler &handler,
                              std::function<bool()> is_leader_fn)
      : handler_(handler), is_leader_fn_(std::move(is_leader_fn)) {}

  // Gated on passive GCS.

  GCS_GATED_RPC(HandleRegisterActor, rpc::RegisterActorRequest, rpc::RegisterActorReply)
  GCS_GATED_RPC(HandleRestartActorForLineageReconstruction,
                rpc::RestartActorForLineageReconstructionRequest,
                rpc::RestartActorForLineageReconstructionReply)
  GCS_GATED_RPC(HandleCreateActor, rpc::CreateActorRequest, rpc::CreateActorReply)
  GCS_GATED_RPC(HandleKillActorViaGcs,
                rpc::KillActorViaGcsRequest,
                rpc::KillActorViaGcsReply)
  GCS_GATED_RPC(HandleReportActorOutOfScope,
                rpc::ReportActorOutOfScopeRequest,
                rpc::ReportActorOutOfScopeReply)
  GCS_GATED_RPC(HandleGetActorInfo, rpc::GetActorInfoRequest, rpc::GetActorInfoReply)
  GCS_GATED_RPC(HandleGetNamedActorInfo,
                rpc::GetNamedActorInfoRequest,
                rpc::GetNamedActorInfoReply)
  GCS_GATED_RPC(HandleListNamedActors,
                rpc::ListNamedActorsRequest,
                rpc::ListNamedActorsReply)
  GCS_GATED_RPC(HandleGetAllActorInfo,
                rpc::GetAllActorInfoRequest,
                rpc::GetAllActorInfoReply)

 private:
  rpc::ActorInfoGcsServiceHandler &handler_;
  const std::function<bool()> is_leader_fn_;
};

class LeaderGatedJobInfoHandler : public rpc::JobInfoGcsServiceHandler {
 public:
  using HandlerType = rpc::JobInfoGcsServiceHandler;
  LeaderGatedJobInfoHandler(rpc::JobInfoGcsServiceHandler &handler,
                            std::function<bool()> is_leader_fn)
      : handler_(handler), is_leader_fn_(std::move(is_leader_fn)) {}

  // Gated on passive GCS.

  GCS_GATED_RPC(HandleAddJob, rpc::AddJobRequest, rpc::AddJobReply)
  GCS_GATED_RPC(HandleMarkJobFinished,
                rpc::MarkJobFinishedRequest,
                rpc::MarkJobFinishedReply)
  GCS_GATED_RPC(HandleGetNextJobID, rpc::GetNextJobIDRequest, rpc::GetNextJobIDReply)
  GCS_GATED_RPC(HandleGetAllJobInfo, rpc::GetAllJobInfoRequest, rpc::GetAllJobInfoReply)

  void AddJobFinishedListener(JobFinishListenerCallback listener) override {
    handler_.AddJobFinishedListener(std::move(listener));
  }

 private:
  rpc::JobInfoGcsServiceHandler &handler_;
  const std::function<bool()> is_leader_fn_;
};

class LeaderGatedPlacementGroupInfoHandler
    : public rpc::PlacementGroupInfoGcsServiceHandler {
 public:
  using HandlerType = rpc::PlacementGroupInfoGcsServiceHandler;
  LeaderGatedPlacementGroupInfoHandler(rpc::PlacementGroupInfoGcsServiceHandler &handler,
                                       std::function<bool()> is_leader_fn)
      : handler_(handler), is_leader_fn_(std::move(is_leader_fn)) {}

  // Gated on passive GCS.

  GCS_GATED_RPC(HandleCreatePlacementGroup,
                rpc::CreatePlacementGroupRequest,
                rpc::CreatePlacementGroupReply)
  GCS_GATED_RPC(HandleRemovePlacementGroup,
                rpc::RemovePlacementGroupRequest,
                rpc::RemovePlacementGroupReply)
  GCS_GATED_RPC(HandleGetPlacementGroup,
                rpc::GetPlacementGroupRequest,
                rpc::GetPlacementGroupReply)
  GCS_GATED_RPC(HandleGetAllPlacementGroup,
                rpc::GetAllPlacementGroupRequest,
                rpc::GetAllPlacementGroupReply)
  GCS_GATED_RPC(HandleWaitPlacementGroupUntilReady,
                rpc::WaitPlacementGroupUntilReadyRequest,
                rpc::WaitPlacementGroupUntilReadyReply)
  GCS_GATED_RPC(HandleGetNamedPlacementGroup,
                rpc::GetNamedPlacementGroupRequest,
                rpc::GetNamedPlacementGroupReply)

 private:
  rpc::PlacementGroupInfoGcsServiceHandler &handler_;
  const std::function<bool()> is_leader_fn_;
};

class LeaderGatedInternalKVHandler : public rpc::InternalKVGcsServiceHandler {
 public:
  using HandlerType = rpc::InternalKVGcsServiceHandler;
  LeaderGatedInternalKVHandler(rpc::InternalKVGcsServiceHandler &handler,
                               std::function<bool()> is_leader_fn)
      : handler_(handler), is_leader_fn_(std::move(is_leader_fn)) {}

  // Gated on passive GCS.

  GCS_GATED_RPC(HandleInternalKVPut, rpc::InternalKVPutRequest, rpc::InternalKVPutReply)
  GCS_GATED_RPC(HandleInternalKVDel, rpc::InternalKVDelRequest, rpc::InternalKVDelReply)
  GCS_GATED_RPC(HandleInternalKVKeys,
                rpc::InternalKVKeysRequest,
                rpc::InternalKVKeysReply)
  GCS_GATED_RPC(HandleInternalKVMultiGet,
                rpc::InternalKVMultiGetRequest,
                rpc::InternalKVMultiGetReply)
  GCS_GATED_RPC(HandleInternalKVExists,
                rpc::InternalKVExistsRequest,
                rpc::InternalKVExistsReply)

  // Allowed on passive GCS (bootstrap).

  GCS_ALLOWED_RPC(HandleInternalKVGet, rpc::InternalKVGetRequest, rpc::InternalKVGetReply)
  GCS_ALLOWED_RPC(HandleGetInternalConfig,
                  rpc::GetInternalConfigRequest,
                  rpc::GetInternalConfigReply)

 private:
  rpc::InternalKVGcsServiceHandler &handler_;
  const std::function<bool()> is_leader_fn_;
};

class LeaderGatedAutoscalerStateHandler
    : public rpc::autoscaler::AutoscalerStateServiceHandler {
 public:
  using HandlerType = rpc::autoscaler::AutoscalerStateServiceHandler;
  LeaderGatedAutoscalerStateHandler(
      rpc::autoscaler::AutoscalerStateServiceHandler &handler,
      std::function<bool()> is_leader_fn)
      : handler_(handler), is_leader_fn_(std::move(is_leader_fn)) {}

  // Gated on passive GCS.

  GCS_GATED_RPC_CB(HandleReportAutoscalingState,
                   rpc::autoscaler::ReportAutoscalingStateRequest,
                   rpc::autoscaler::ReportAutoscalingStateReply)
  GCS_GATED_RPC_CB(HandleRequestClusterResourceConstraint,
                   rpc::autoscaler::RequestClusterResourceConstraintRequest,
                   rpc::autoscaler::RequestClusterResourceConstraintReply)
  GCS_GATED_RPC_CB_PEER(HandleDrainNode,
                        rpc::autoscaler::DrainNodeRequest,
                        rpc::autoscaler::DrainNodeReply)
  GCS_GATED_RPC_CB(HandleResizeRayletResourceInstances,
                   rpc::autoscaler::ResizeRayletResourceInstancesRequest,
                   rpc::autoscaler::ResizeRayletResourceInstancesReply)
  GCS_GATED_RPC_CB(HandleReportClusterConfig,
                   rpc::autoscaler::ReportClusterConfigRequest,
                   rpc::autoscaler::ReportClusterConfigReply)
  GCS_GATED_RPC_CB(HandleGetClusterResourceState,
                   rpc::autoscaler::GetClusterResourceStateRequest,
                   rpc::autoscaler::GetClusterResourceStateReply)
  GCS_GATED_RPC_CB(HandleGetClusterStatus,
                   rpc::autoscaler::GetClusterStatusRequest,
                   rpc::autoscaler::GetClusterStatusReply)

 private:
  rpc::autoscaler::AutoscalerStateServiceHandler &handler_;
  const std::function<bool()> is_leader_fn_;
};

class LeaderGatedNodeResourceInfoHandler : public rpc::NodeResourceInfoGcsServiceHandler {
 public:
  using HandlerType = rpc::NodeResourceInfoGcsServiceHandler;
  LeaderGatedNodeResourceInfoHandler(rpc::NodeResourceInfoGcsServiceHandler &handler,
                                     std::function<bool()> is_leader_fn)
      : handler_(handler), is_leader_fn_(std::move(is_leader_fn)) {}

  // Gated: NodeResourceInfo consumers (autoscaler/state observers) are
  // suppressed on a passive head, so none are needed until promotion.

  GCS_GATED_RPC(HandleGetAllAvailableResources,
                rpc::GetAllAvailableResourcesRequest,
                rpc::GetAllAvailableResourcesReply)
  GCS_GATED_RPC(HandleGetAllTotalResources,
                rpc::GetAllTotalResourcesRequest,
                rpc::GetAllTotalResourcesReply)
  GCS_GATED_RPC(HandleGetDrainingNodes,
                rpc::GetDrainingNodesRequest,
                rpc::GetDrainingNodesReply)
  GCS_GATED_RPC(HandleGetAllResourceUsage,
                rpc::GetAllResourceUsageRequest,
                rpc::GetAllResourceUsageReply)

 private:
  rpc::NodeResourceInfoGcsServiceHandler &handler_;
  const std::function<bool()> is_leader_fn_;
};

class LeaderGatedWorkerInfoHandler : public rpc::WorkerInfoGcsServiceHandler {
 public:
  using HandlerType = rpc::WorkerInfoGcsServiceHandler;
  LeaderGatedWorkerInfoHandler(rpc::WorkerInfoGcsServiceHandler &handler,
                               std::function<bool()> is_leader_fn)
      : handler_(handler), is_leader_fn_(std::move(is_leader_fn)) {}

  // Gated on passive GCS.

  GCS_GATED_RPC(HandleReportWorkerFailure,
                rpc::ReportWorkerFailureRequest,
                rpc::ReportWorkerFailureReply)
  GCS_GATED_RPC(HandleAddWorkerInfo, rpc::AddWorkerInfoRequest, rpc::AddWorkerInfoReply)
  GCS_GATED_RPC(HandleUpdateWorkerDebuggerPort,
                rpc::UpdateWorkerDebuggerPortRequest,
                rpc::UpdateWorkerDebuggerPortReply)
  GCS_GATED_RPC(HandleUpdateWorkerNumPausedThreads,
                rpc::UpdateWorkerNumPausedThreadsRequest,
                rpc::UpdateWorkerNumPausedThreadsReply)

  // Reads are also gated: no workers run on a passive head.
  GCS_GATED_RPC(HandleGetWorkerInfo, rpc::GetWorkerInfoRequest, rpc::GetWorkerInfoReply)
  GCS_GATED_RPC(HandleGetAllWorkerInfo,
                rpc::GetAllWorkerInfoRequest,
                rpc::GetAllWorkerInfoReply)

 private:
  rpc::WorkerInfoGcsServiceHandler &handler_;
  const std::function<bool()> is_leader_fn_;
};

class LeaderGatedTaskInfoHandler : public rpc::TaskInfoGcsServiceHandler {
 public:
  using HandlerType = rpc::TaskInfoGcsServiceHandler;
  LeaderGatedTaskInfoHandler(rpc::TaskInfoGcsServiceHandler &handler,
                             std::function<bool()> is_leader_fn)
      : handler_(handler), is_leader_fn_(std::move(is_leader_fn)) {}

  // Gated on passive GCS.

  GCS_GATED_RPC(HandleAddTaskEventData,
                rpc::AddTaskEventDataRequest,
                rpc::AddTaskEventDataReply)

  // Reads are also gated: no tasks run on a passive head.
  GCS_GATED_RPC(HandleGetTaskEvents, rpc::GetTaskEventsRequest, rpc::GetTaskEventsReply)

 private:
  rpc::TaskInfoGcsServiceHandler &handler_;
  const std::function<bool()> is_leader_fn_;
};

class LeaderGatedRuntimeEnvHandler : public rpc::RuntimeEnvGcsServiceHandler {
 public:
  using HandlerType = rpc::RuntimeEnvGcsServiceHandler;
  LeaderGatedRuntimeEnvHandler(rpc::RuntimeEnvGcsServiceHandler &handler,
                               std::function<bool()> is_leader_fn)
      : handler_(handler), is_leader_fn_(std::move(is_leader_fn)) {}

  // Gated: pinning a runtime env URI writes to the shared GCS KV store.

  GCS_GATED_RPC(HandlePinRuntimeEnvURI,
                rpc::PinRuntimeEnvURIRequest,
                rpc::PinRuntimeEnvURIReply)

 private:
  rpc::RuntimeEnvGcsServiceHandler &handler_;
  const std::function<bool()> is_leader_fn_;
};

class LeaderGatedControlPlanePubSubHandler
    : public rpc::ControlPlanePubSubGcsServiceHandler {
 public:
  using HandlerType = rpc::ControlPlanePubSubGcsServiceHandler;
  LeaderGatedControlPlanePubSubHandler(rpc::ControlPlanePubSubGcsServiceHandler &handler,
                                       std::function<bool()> is_leader_fn)
      : handler_(handler), is_leader_fn_(std::move(is_leader_fn)) {}

  // Gated: publishing mutates control-plane state; owned by the active GCS.
  GCS_GATED_RPC(HandleGcsPublish, rpc::GcsPublishRequest, rpc::GcsPublishReply)

  // Allowed: the local raylet's node address/liveness subscription during
  // startup runs on top of these; without them it cannot boot on a passive head.

  GCS_ALLOWED_RPC(HandleGcsSubscriberPoll,
                  rpc::GcsSubscriberPollRequest,
                  rpc::GcsSubscriberPollReply)
  GCS_ALLOWED_RPC(HandleGcsSubscriberCommandBatch,
                  rpc::GcsSubscriberCommandBatchRequest,
                  rpc::GcsSubscriberCommandBatchReply)

 private:
  rpc::ControlPlanePubSubGcsServiceHandler &handler_;
  const std::function<bool()> is_leader_fn_;
};

class LeaderGatedObservabilityPubSubHandler
    : public rpc::ObservabilityPubSubServiceHandler {
 public:
  using HandlerType = rpc::ObservabilityPubSubServiceHandler;
  LeaderGatedObservabilityPubSubHandler(rpc::ObservabilityPubSubServiceHandler &handler,
                                        std::function<bool()> is_leader_fn)
      : handler_(handler), is_leader_fn_(std::move(is_leader_fn)) {}

  // Gated: publishing and reporting job errors mutate control-plane state.
  GCS_GATED_RPC(HandleGcsPublish, rpc::GcsPublishRequest, rpc::GcsPublishReply)
  GCS_GATED_RPC(HandleReportJobError,
                rpc::ReportJobErrorRequest,
                rpc::ReportJobErrorReply)

  // Allowed: read-only subscribe/long-poll, so local observability subscribers
  // can attach (mirrors the control-plane pubsub allowlist).

  GCS_ALLOWED_RPC(HandleGcsSubscriberPoll,
                  rpc::GcsSubscriberPollRequest,
                  rpc::GcsSubscriberPollReply)
  GCS_ALLOWED_RPC(HandleGcsSubscriberCommandBatch,
                  rpc::GcsSubscriberCommandBatchRequest,
                  rpc::GcsSubscriberCommandBatchReply)

 private:
  rpc::ObservabilityPubSubServiceHandler &handler_;
  const std::function<bool()> is_leader_fn_;
};

class LeaderGatedRayEventExportHandler
    : public rpc::events::RayEventExportGcsServiceHandler {
 public:
  using HandlerType = rpc::events::RayEventExportGcsServiceHandler;
  LeaderGatedRayEventExportHandler(rpc::events::RayEventExportGcsServiceHandler &handler,
                                   std::function<bool()> is_leader_fn)
      : handler_(handler), is_leader_fn_(std::move(is_leader_fn)) {}

  // Gated: AddEvents ingests into the task manager; no workers run on a passive
  // head, so it is gated until promotion.

  GCS_GATED_RPC(HandleAddEvents,
                rpc::events::AddEventsRequest,
                rpc::events::AddEventsReply)

 private:
  rpc::events::RayEventExportGcsServiceHandler &handler_;
  const std::function<bool()> is_leader_fn_;
};

#undef GCS_PROXY_SEND_REPLY
#undef GCS_GATED_RPC
#undef GCS_GATED_RPC_PEER
#undef GCS_ALLOWED_RPC
#undef GCS_GATED_RPC_CB
#undef GCS_GATED_RPC_CB_PEER

}  // namespace gcs
}  // namespace ray
