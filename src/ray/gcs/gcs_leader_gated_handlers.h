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
#include "ray/util/network_util.h"

namespace ray {
namespace gcs {

// Helper macro to send reply status
#define GCS_PROXY_SEND_REPLY(send_reply_callback, reply, status)      \
  reply->mutable_status()->set_code(static_cast<int>(status.code())); \
  reply->mutable_status()->set_message(status.message());             \
  send_reply_callback(ray::Status::OK(), nullptr, nullptr)

class LeaderGatedNodeInfoHandler : public rpc::NodeInfoGcsServiceHandler {
 public:
  LeaderGatedNodeInfoHandler(rpc::NodeInfoGcsServiceHandler &handler,
                             std::function<bool()> is_leader_fn)
      : handler_(handler),
        is_leader_fn_(std::move(is_leader_fn)),
        local_ip_(GetNodeIpAddressFromPerspective()) {}

  // =========================================================================
  // Gated Mutating RPCs (Blocked on passive GCS, returns Status::GcsPassive)
  // =========================================================================

  void HandleRegisterNode(rpc::RegisterNodeRequest request,
                          rpc::RegisterNodeReply *reply,
                          rpc::SendReplyCallback send_reply_callback) override {
    const std::string &node_ip = request.node_info().node_manager_address();
    const bool is_local = (node_ip == local_ip_) || (node_ip == "127.0.0.1") ||
                          (node_ip == "localhost") || (node_ip == "::1");
    if (!is_leader_fn_() && !is_local) {
      GCS_PROXY_SEND_REPLY(send_reply_callback, reply, Status::GcsPassive());
      return;
    }
    handler_.HandleRegisterNode(
        std::move(request), reply, std::move(send_reply_callback));
  }

  void HandleUnregisterNode(rpc::UnregisterNodeRequest request,
                            rpc::UnregisterNodeReply *reply,
                            rpc::SendReplyCallback send_reply_callback,
                            const std::string &grpc_peer) override {
    if (!is_leader_fn_()) {
      GCS_PROXY_SEND_REPLY(send_reply_callback, reply, Status::GcsPassive());
      return;
    }
    handler_.HandleUnregisterNode(
        std::move(request), reply, std::move(send_reply_callback), grpc_peer);
  }

  void HandleDrainNode(rpc::DrainNodeRequest request,
                       rpc::DrainNodeReply *reply,
                       rpc::SendReplyCallback send_reply_callback) override {
    if (!is_leader_fn_()) {
      GCS_PROXY_SEND_REPLY(send_reply_callback, reply, Status::GcsPassive());
      return;
    }
    handler_.HandleDrainNode(std::move(request), reply, std::move(send_reply_callback));
  }

  // =========================================================================
  // Pass-Through Read-Only RPCs (Allowed on passive GCS)
  // =========================================================================

  void HandleGetClusterId(rpc::GetClusterIdRequest request,
                          rpc::GetClusterIdReply *reply,
                          rpc::SendReplyCallback send_reply_callback) override {
    handler_.HandleGetClusterId(
        std::move(request), reply, std::move(send_reply_callback));
  }

  void HandleCheckAlive(rpc::CheckAliveRequest request,
                        rpc::CheckAliveReply *reply,
                        rpc::SendReplyCallback send_reply_callback) override {
    handler_.HandleCheckAlive(std::move(request), reply, std::move(send_reply_callback));
  }

  void HandleGetAllNodeInfo(rpc::GetAllNodeInfoRequest request,
                            rpc::GetAllNodeInfoReply *reply,
                            rpc::SendReplyCallback send_reply_callback) override {
    handler_.HandleGetAllNodeInfo(
        std::move(request), reply, std::move(send_reply_callback));
  }

  void HandleGetAllNodeAddressAndLiveness(
      rpc::GetAllNodeAddressAndLivenessRequest request,
      rpc::GetAllNodeAddressAndLivenessReply *reply,
      rpc::SendReplyCallback send_reply_callback) override {
    handler_.HandleGetAllNodeAddressAndLiveness(
        std::move(request), reply, std::move(send_reply_callback));
  }

 private:
  rpc::NodeInfoGcsServiceHandler &handler_;
  const std::function<bool()> is_leader_fn_;
  const std::string local_ip_;
};

class LeaderGatedActorInfoHandler : public rpc::ActorInfoGcsServiceHandler {
 public:
  LeaderGatedActorInfoHandler(rpc::ActorInfoGcsServiceHandler &handler,
                              std::function<bool()> is_leader_fn)
      : handler_(handler), is_leader_fn_(std::move(is_leader_fn)) {}

  // =========================================================================
  // Gated Mutating RPCs (Blocked on passive GCS, returns Status::GcsPassive)
  // =========================================================================

  void HandleRegisterActor(rpc::RegisterActorRequest request,
                           rpc::RegisterActorReply *reply,
                           rpc::SendReplyCallback send_reply_callback) override {
    if (!is_leader_fn_()) {
      GCS_PROXY_SEND_REPLY(send_reply_callback, reply, Status::GcsPassive());
      return;
    }
    handler_.HandleRegisterActor(
        std::move(request), reply, std::move(send_reply_callback));
  }

  void HandleRestartActorForLineageReconstruction(
      rpc::RestartActorForLineageReconstructionRequest request,
      rpc::RestartActorForLineageReconstructionReply *reply,
      rpc::SendReplyCallback send_reply_callback) override {
    if (!is_leader_fn_()) {
      GCS_PROXY_SEND_REPLY(send_reply_callback, reply, Status::GcsPassive());
      return;
    }
    handler_.HandleRestartActorForLineageReconstruction(
        std::move(request), reply, std::move(send_reply_callback));
  }

  void HandleCreateActor(rpc::CreateActorRequest request,
                         rpc::CreateActorReply *reply,
                         rpc::SendReplyCallback send_reply_callback) override {
    if (!is_leader_fn_()) {
      GCS_PROXY_SEND_REPLY(send_reply_callback, reply, Status::GcsPassive());
      return;
    }
    handler_.HandleCreateActor(std::move(request), reply, std::move(send_reply_callback));
  }

  void HandleKillActorViaGcs(rpc::KillActorViaGcsRequest request,
                             rpc::KillActorViaGcsReply *reply,
                             rpc::SendReplyCallback send_reply_callback) override {
    if (!is_leader_fn_()) {
      GCS_PROXY_SEND_REPLY(send_reply_callback, reply, Status::GcsPassive());
      return;
    }
    handler_.HandleKillActorViaGcs(
        std::move(request), reply, std::move(send_reply_callback));
  }

  void HandleReportActorOutOfScope(rpc::ReportActorOutOfScopeRequest request,
                                   rpc::ReportActorOutOfScopeReply *reply,
                                   rpc::SendReplyCallback send_reply_callback) override {
    if (!is_leader_fn_()) {
      GCS_PROXY_SEND_REPLY(send_reply_callback, reply, Status::GcsPassive());
      return;
    }
    handler_.HandleReportActorOutOfScope(
        std::move(request), reply, std::move(send_reply_callback));
  }

  // =========================================================================
  // Pass-Through Read-Only RPCs (Allowed on passive GCS)
  // =========================================================================

  void HandleGetActorInfo(rpc::GetActorInfoRequest request,
                          rpc::GetActorInfoReply *reply,
                          rpc::SendReplyCallback send_reply_callback) override {
    handler_.HandleGetActorInfo(
        std::move(request), reply, std::move(send_reply_callback));
  }

  void HandleGetNamedActorInfo(rpc::GetNamedActorInfoRequest request,
                               rpc::GetNamedActorInfoReply *reply,
                               rpc::SendReplyCallback send_reply_callback) override {
    handler_.HandleGetNamedActorInfo(
        std::move(request), reply, std::move(send_reply_callback));
  }

  void HandleListNamedActors(rpc::ListNamedActorsRequest request,
                             rpc::ListNamedActorsReply *reply,
                             rpc::SendReplyCallback send_reply_callback) override {
    handler_.HandleListNamedActors(
        std::move(request), reply, std::move(send_reply_callback));
  }

  void HandleGetAllActorInfo(rpc::GetAllActorInfoRequest request,
                             rpc::GetAllActorInfoReply *reply,
                             rpc::SendReplyCallback send_reply_callback) override {
    handler_.HandleGetAllActorInfo(
        std::move(request), reply, std::move(send_reply_callback));
  }

 private:
  rpc::ActorInfoGcsServiceHandler &handler_;
  const std::function<bool()> is_leader_fn_;
};

class LeaderGatedJobInfoHandler : public rpc::JobInfoGcsServiceHandler {
 public:
  LeaderGatedJobInfoHandler(rpc::JobInfoGcsServiceHandler &handler,
                            std::function<bool()> is_leader_fn)
      : handler_(handler), is_leader_fn_(std::move(is_leader_fn)) {}

  // =========================================================================
  // Gated Mutating RPCs (Blocked on passive GCS, returns Status::GcsPassive)
  // =========================================================================

  void HandleAddJob(rpc::AddJobRequest request,
                    rpc::AddJobReply *reply,
                    rpc::SendReplyCallback send_reply_callback) override {
    if (!is_leader_fn_()) {
      GCS_PROXY_SEND_REPLY(send_reply_callback, reply, Status::GcsPassive());
      return;
    }
    handler_.HandleAddJob(std::move(request), reply, std::move(send_reply_callback));
  }

  void HandleMarkJobFinished(rpc::MarkJobFinishedRequest request,
                             rpc::MarkJobFinishedReply *reply,
                             rpc::SendReplyCallback send_reply_callback) override {
    if (!is_leader_fn_()) {
      GCS_PROXY_SEND_REPLY(send_reply_callback, reply, Status::GcsPassive());
      return;
    }
    handler_.HandleMarkJobFinished(
        std::move(request), reply, std::move(send_reply_callback));
  }

  void HandleGetNextJobID(rpc::GetNextJobIDRequest request,
                          rpc::GetNextJobIDReply *reply,
                          rpc::SendReplyCallback send_reply_callback) override {
    if (!is_leader_fn_()) {
      GCS_PROXY_SEND_REPLY(send_reply_callback, reply, Status::GcsPassive());
      return;
    }
    handler_.HandleGetNextJobID(
        std::move(request), reply, std::move(send_reply_callback));
  }

  // =========================================================================
  // Pass-Through Read-Only RPCs (Allowed on passive GCS)
  // =========================================================================

  void HandleGetAllJobInfo(rpc::GetAllJobInfoRequest request,
                           rpc::GetAllJobInfoReply *reply,
                           rpc::SendReplyCallback send_reply_callback) override {
    handler_.HandleGetAllJobInfo(
        std::move(request), reply, std::move(send_reply_callback));
  }

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
  LeaderGatedPlacementGroupInfoHandler(rpc::PlacementGroupInfoGcsServiceHandler &handler,
                                       std::function<bool()> is_leader_fn)
      : handler_(handler), is_leader_fn_(std::move(is_leader_fn)) {}

  // =========================================================================
  // Gated Mutating RPCs (Blocked on passive GCS, returns Status::GcsPassive)
  // =========================================================================

  void HandleCreatePlacementGroup(rpc::CreatePlacementGroupRequest request,
                                  rpc::CreatePlacementGroupReply *reply,
                                  rpc::SendReplyCallback send_reply_callback) override {
    if (!is_leader_fn_()) {
      GCS_PROXY_SEND_REPLY(send_reply_callback, reply, Status::GcsPassive());
      return;
    }
    handler_.HandleCreatePlacementGroup(
        std::move(request), reply, std::move(send_reply_callback));
  }

  void HandleRemovePlacementGroup(rpc::RemovePlacementGroupRequest request,
                                  rpc::RemovePlacementGroupReply *reply,
                                  rpc::SendReplyCallback send_reply_callback) override {
    if (!is_leader_fn_()) {
      GCS_PROXY_SEND_REPLY(send_reply_callback, reply, Status::GcsPassive());
      return;
    }
    handler_.HandleRemovePlacementGroup(
        std::move(request), reply, std::move(send_reply_callback));
  }

  // =========================================================================
  // Pass-Through Read-Only RPCs (Allowed on passive GCS)
  // =========================================================================

  void HandleGetPlacementGroup(rpc::GetPlacementGroupRequest request,
                               rpc::GetPlacementGroupReply *reply,
                               rpc::SendReplyCallback send_reply_callback) override {
    handler_.HandleGetPlacementGroup(
        std::move(request), reply, std::move(send_reply_callback));
  }

  void HandleGetAllPlacementGroup(rpc::GetAllPlacementGroupRequest request,
                                  rpc::GetAllPlacementGroupReply *reply,
                                  rpc::SendReplyCallback send_reply_callback) override {
    handler_.HandleGetAllPlacementGroup(
        std::move(request), reply, std::move(send_reply_callback));
  }

  void HandleWaitPlacementGroupUntilReady(
      rpc::WaitPlacementGroupUntilReadyRequest request,
      rpc::WaitPlacementGroupUntilReadyReply *reply,
      rpc::SendReplyCallback send_reply_callback) override {
    handler_.HandleWaitPlacementGroupUntilReady(
        std::move(request), reply, std::move(send_reply_callback));
  }

  void HandleGetNamedPlacementGroup(rpc::GetNamedPlacementGroupRequest request,
                                    rpc::GetNamedPlacementGroupReply *reply,
                                    rpc::SendReplyCallback send_reply_callback) override {
    handler_.HandleGetNamedPlacementGroup(
        std::move(request), reply, std::move(send_reply_callback));
  }

 private:
  rpc::PlacementGroupInfoGcsServiceHandler &handler_;
  const std::function<bool()> is_leader_fn_;
};

class LeaderGatedInternalKVHandler : public rpc::InternalKVGcsServiceHandler {
 public:
  LeaderGatedInternalKVHandler(rpc::InternalKVGcsServiceHandler &handler,
                               std::function<bool()> is_leader_fn)
      : handler_(handler), is_leader_fn_(std::move(is_leader_fn)) {}

  // =========================================================================
  // Gated Mutating RPCs (Blocked on passive GCS, returns Status::GcsPassive)
  // =========================================================================

  void HandleInternalKVPut(rpc::InternalKVPutRequest request,
                           rpc::InternalKVPutReply *reply,
                           rpc::SendReplyCallback send_reply_callback) override {
    if (!is_leader_fn_()) {
      GCS_PROXY_SEND_REPLY(send_reply_callback, reply, Status::GcsPassive());
      return;
    }
    handler_.HandleInternalKVPut(
        std::move(request), reply, std::move(send_reply_callback));
  }

  void HandleInternalKVDel(rpc::InternalKVDelRequest request,
                           rpc::InternalKVDelReply *reply,
                           rpc::SendReplyCallback send_reply_callback) override {
    if (!is_leader_fn_()) {
      GCS_PROXY_SEND_REPLY(send_reply_callback, reply, Status::GcsPassive());
      return;
    }
    handler_.HandleInternalKVDel(
        std::move(request), reply, std::move(send_reply_callback));
  }

  // =========================================================================
  // Pass-Through Read-Only RPCs (Allowed on passive GCS)
  // =========================================================================

  void HandleInternalKVKeys(rpc::InternalKVKeysRequest request,
                            rpc::InternalKVKeysReply *reply,
                            rpc::SendReplyCallback send_reply_callback) override {
    handler_.HandleInternalKVKeys(
        std::move(request), reply, std::move(send_reply_callback));
  }

  void HandleInternalKVGet(rpc::InternalKVGetRequest request,
                           rpc::InternalKVGetReply *reply,
                           rpc::SendReplyCallback send_reply_callback) override {
    handler_.HandleInternalKVGet(
        std::move(request), reply, std::move(send_reply_callback));
  }

  void HandleInternalKVMultiGet(rpc::InternalKVMultiGetRequest request,
                                rpc::InternalKVMultiGetReply *reply,
                                rpc::SendReplyCallback send_reply_callback) override {
    handler_.HandleInternalKVMultiGet(
        std::move(request), reply, std::move(send_reply_callback));
  }

  void HandleInternalKVExists(rpc::InternalKVExistsRequest request,
                              rpc::InternalKVExistsReply *reply,
                              rpc::SendReplyCallback send_reply_callback) override {
    handler_.HandleInternalKVExists(
        std::move(request), reply, std::move(send_reply_callback));
  }

  void HandleGetInternalConfig(rpc::GetInternalConfigRequest request,
                               rpc::GetInternalConfigReply *reply,
                               rpc::SendReplyCallback send_reply_callback) override {
    handler_.HandleGetInternalConfig(
        std::move(request), reply, std::move(send_reply_callback));
  }

 private:
  rpc::InternalKVGcsServiceHandler &handler_;
  const std::function<bool()> is_leader_fn_;
};

class LeaderGatedAutoscalerStateHandler
    : public rpc::autoscaler::AutoscalerStateServiceHandler {
 public:
  LeaderGatedAutoscalerStateHandler(
      rpc::autoscaler::AutoscalerStateServiceHandler &handler,
      std::function<bool()> is_leader_fn)
      : handler_(handler), is_leader_fn_(std::move(is_leader_fn)) {}

  // =========================================================================
  // Gated Mutating RPCs (Blocked on passive GCS, returns Status::GcsPassive)
  // =========================================================================

  void HandleReportAutoscalingState(
      rpc::autoscaler::ReportAutoscalingStateRequest request,
      rpc::autoscaler::ReportAutoscalingStateReply *reply,
      rpc::SendReplyCallback send_reply_callback) override {
    if (!is_leader_fn_()) {
      send_reply_callback(Status::GcsPassive(), nullptr, nullptr);
      return;
    }
    handler_.HandleReportAutoscalingState(
        std::move(request), reply, std::move(send_reply_callback));
  }

  void HandleRequestClusterResourceConstraint(
      rpc::autoscaler::RequestClusterResourceConstraintRequest request,
      rpc::autoscaler::RequestClusterResourceConstraintReply *reply,
      rpc::SendReplyCallback send_reply_callback) override {
    if (!is_leader_fn_()) {
      send_reply_callback(Status::GcsPassive(), nullptr, nullptr);
      return;
    }
    handler_.HandleRequestClusterResourceConstraint(
        std::move(request), reply, std::move(send_reply_callback));
  }

  void HandleDrainNode(rpc::autoscaler::DrainNodeRequest request,
                       rpc::autoscaler::DrainNodeReply *reply,
                       rpc::SendReplyCallback send_reply_callback,
                       const std::string &grpc_peer) override {
    if (!is_leader_fn_()) {
      send_reply_callback(Status::GcsPassive(), nullptr, nullptr);
      return;
    }
    handler_.HandleDrainNode(
        std::move(request), reply, std::move(send_reply_callback), grpc_peer);
  }

  void HandleResizeRayletResourceInstances(
      rpc::autoscaler::ResizeRayletResourceInstancesRequest request,
      rpc::autoscaler::ResizeRayletResourceInstancesReply *reply,
      rpc::SendReplyCallback send_reply_callback) override {
    if (!is_leader_fn_()) {
      send_reply_callback(Status::GcsPassive(), nullptr, nullptr);
      return;
    }
    handler_.HandleResizeRayletResourceInstances(
        std::move(request), reply, std::move(send_reply_callback));
  }

  void HandleReportClusterConfig(rpc::autoscaler::ReportClusterConfigRequest request,
                                 rpc::autoscaler::ReportClusterConfigReply *reply,
                                 rpc::SendReplyCallback send_reply_callback) override {
    if (!is_leader_fn_()) {
      send_reply_callback(Status::GcsPassive(), nullptr, nullptr);
      return;
    }
    handler_.HandleReportClusterConfig(
        std::move(request), reply, std::move(send_reply_callback));
  }

  // =========================================================================
  // Pass-Through Read-Only RPCs (Allowed on passive GCS)
  // =========================================================================

  void HandleGetClusterResourceState(
      rpc::autoscaler::GetClusterResourceStateRequest request,
      rpc::autoscaler::GetClusterResourceStateReply *reply,
      rpc::SendReplyCallback send_reply_callback) override {
    handler_.HandleGetClusterResourceState(
        std::move(request), reply, std::move(send_reply_callback));
  }

  void HandleGetClusterStatus(rpc::autoscaler::GetClusterStatusRequest request,
                              rpc::autoscaler::GetClusterStatusReply *reply,
                              rpc::SendReplyCallback send_reply_callback) override {
    handler_.HandleGetClusterStatus(
        std::move(request), reply, std::move(send_reply_callback));
  }

 private:
  rpc::autoscaler::AutoscalerStateServiceHandler &handler_;
  const std::function<bool()> is_leader_fn_;
};

#undef GCS_PROXY_SEND_REPLY

}  // namespace gcs
}  // namespace ray
