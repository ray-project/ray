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

/*
 * This file defines the gRPC service *INTERFACES* only.
 * The subcomponent that handles a given interface should inherit from the relevant
 * class. The target for the subcomponent should depend only on this file, not on
 * grpc_services.h.
 */

#pragma once

#include "ray/common/status.h"
#include "ray/rpc/rpc_callback_types.h"
#include "src/ray/protobuf/autoscaler.grpc.pb.h"
#include "src/ray/protobuf/gcs_service.grpc.pb.h"

namespace ray {
namespace rpc {

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

class NodeInfoGcsServiceHandler {
 public:
  virtual ~NodeInfoGcsServiceHandler() = default;

  virtual void HandleGetClusterId(GetClusterIdRequest request,
                                  GetClusterIdReply *reply,
                                  SendReplyCallback send_reply_callback) = 0;

  virtual void HandleRegisterNode(RegisterNodeRequest request,
                                  RegisterNodeReply *reply,
                                  SendReplyCallback send_reply_callback) = 0;

  virtual void HandleUnregisterNode(UnregisterNodeRequest request,
                                    UnregisterNodeReply *reply,
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
};

class NodeResourceInfoGcsServiceHandler {
 public:
  virtual ~NodeResourceInfoGcsServiceHandler() = default;

  virtual void HandleGetAllAvailableResources(GetAllAvailableResourcesRequest request,
                                              GetAllAvailableResourcesReply *reply,
                                              SendReplyCallback send_reply_callback) = 0;

  virtual void HandleGetAllTotalResources(GetAllTotalResourcesRequest request,
                                          GetAllTotalResourcesReply *reply,
                                          SendReplyCallback send_reply_callback) = 0;

  virtual void HandleGetDrainingNodes(GetDrainingNodesRequest request,
                                      GetDrainingNodesReply *reply,
                                      SendReplyCallback send_reply_callback) = 0;

  virtual void HandleGetAllResourceUsage(GetAllResourceUsageRequest request,
                                         GetAllResourceUsageReply *reply,
                                         SendReplyCallback send_reply_callback) = 0;
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

class JobInfoGcsServiceHandler {
 public:
  using JobFinishListenerCallback = std::function<void(const rpc::JobTableData &)>;

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

  virtual void AddJobFinishedListener(JobFinishListenerCallback listener) = 0;

  virtual void HandleReportJobError(ReportJobErrorRequest request,
                                    ReportJobErrorReply *reply,
                                    SendReplyCallback send_reply_callback) = 0;

  virtual void HandleGetNextJobID(GetNextJobIDRequest request,
                                  GetNextJobIDReply *reply,
                                  SendReplyCallback send_reply_callback) = 0;
};

class RuntimeEnvGcsServiceHandler {
 public:
  virtual ~RuntimeEnvGcsServiceHandler() = default;

  virtual void HandlePinRuntimeEnvURI(PinRuntimeEnvURIRequest request,
                                      PinRuntimeEnvURIReply *reply,
                                      SendReplyCallback send_reply_callback) = 0;
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

  virtual void HandleUpdateWorkerDebuggerPort(UpdateWorkerDebuggerPortRequest request,
                                              UpdateWorkerDebuggerPortReply *reply,
                                              SendReplyCallback send_reply_callback) = 0;

  virtual void HandleUpdateWorkerNumPausedThreads(
      UpdateWorkerNumPausedThreadsRequest request,
      UpdateWorkerNumPausedThreadsReply *reply,
      SendReplyCallback send_reply_callback) = 0;
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

  virtual void HandleInternalKVMultiGet(InternalKVMultiGetRequest request,
                                        InternalKVMultiGetReply *reply,
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

  virtual void HandleGetInternalConfig(GetInternalConfigRequest request,
                                       GetInternalConfigReply *reply,
                                       SendReplyCallback send_reply_callback) = 0;
};

class TaskInfoGcsServiceHandler {
 public:
  virtual ~TaskInfoGcsServiceHandler() = default;

  virtual void HandleAddTaskEventData(AddTaskEventDataRequest request,
                                      AddTaskEventDataReply *reply,
                                      SendReplyCallback send_reply_callback) = 0;

  virtual void HandleGetTaskEvents(GetTaskEventsRequest request,
                                   GetTaskEventsReply *reply,
                                   SendReplyCallback send_reply_callback) = 0;
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

namespace autoscaler {

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

}  // namespace autoscaler

namespace events {

class RayEventExportGcsServiceHandler {
 public:
  virtual ~RayEventExportGcsServiceHandler() = default;
  virtual void HandleAddEvents(events::AddEventsRequest request,
                               events::AddEventsReply *reply,
                               SendReplyCallback send_reply_callback) = 0;
};

}  // namespace events

}  // namespace rpc
}  // namespace ray
