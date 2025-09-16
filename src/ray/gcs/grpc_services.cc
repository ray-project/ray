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
#include "ray/gcs/grpc_services.h"

#include <memory>
#include <vector>

namespace ray {
namespace rpc {

void ActorInfoGrpcService::InitServerCallFactories(
    const std::unique_ptr<grpc::ServerCompletionQueue> &cq,
    std::vector<std::unique_ptr<ServerCallFactory>> *server_call_factories,
    const ClusterID &cluster_id) {
  /// The register & create actor RPCs take a long time, so we shouldn't limit their
  /// concurrency to avoid distributed deadlock.
  RPC_SERVICE_HANDLER(ActorInfoGcsService, RegisterActor, -1)
  RPC_SERVICE_HANDLER(ActorInfoGcsService, CreateActor, -1)
  RPC_SERVICE_HANDLER(ActorInfoGcsService, RestartActorForLineageReconstruction, -1)

  RPC_SERVICE_HANDLER(ActorInfoGcsService, GetActorInfo, max_active_rpcs_per_handler_)
  RPC_SERVICE_HANDLER(ActorInfoGcsService, GetAllActorInfo, max_active_rpcs_per_handler_)
  RPC_SERVICE_HANDLER(
      ActorInfoGcsService, GetNamedActorInfo, max_active_rpcs_per_handler_)
  RPC_SERVICE_HANDLER(ActorInfoGcsService, ListNamedActors, max_active_rpcs_per_handler_)
  RPC_SERVICE_HANDLER(ActorInfoGcsService, KillActorViaGcs, max_active_rpcs_per_handler_)
  RPC_SERVICE_HANDLER(
      ActorInfoGcsService, ReportActorOutOfScope, max_active_rpcs_per_handler_)
}

void NodeInfoGrpcService::InitServerCallFactories(
    const std::unique_ptr<grpc::ServerCompletionQueue> &cq,
    std::vector<std::unique_ptr<ServerCallFactory>> *server_call_factories,
    const ClusterID &cluster_id) {
  // We only allow one cluster ID in the lifetime of a client.
  // So, if a client connects, it should not have a pre-existing different ID.
  RPC_SERVICE_HANDLER_CUSTOM_AUTH(NodeInfoGcsService,
                                  GetClusterId,
                                  max_active_rpcs_per_handler_,
                                  AuthType::EMPTY_AUTH);
  RPC_SERVICE_HANDLER(NodeInfoGcsService, RegisterNode, max_active_rpcs_per_handler_)
  RPC_SERVICE_HANDLER(NodeInfoGcsService, UnregisterNode, max_active_rpcs_per_handler_)
  RPC_SERVICE_HANDLER(NodeInfoGcsService, DrainNode, max_active_rpcs_per_handler_)
  RPC_SERVICE_HANDLER(NodeInfoGcsService, GetAllNodeInfo, max_active_rpcs_per_handler_)
  RPC_SERVICE_HANDLER(NodeInfoGcsService, CheckAlive, max_active_rpcs_per_handler_)
}

void NodeResourceInfoGrpcService::InitServerCallFactories(
    const std::unique_ptr<grpc::ServerCompletionQueue> &cq,
    std::vector<std::unique_ptr<ServerCallFactory>> *server_call_factories,
    const ClusterID &cluster_id) {
  RPC_SERVICE_HANDLER(
      NodeResourceInfoGcsService, GetAllAvailableResources, max_active_rpcs_per_handler_)
  RPC_SERVICE_HANDLER(
      NodeResourceInfoGcsService, GetAllTotalResources, max_active_rpcs_per_handler_)
  RPC_SERVICE_HANDLER(
      NodeResourceInfoGcsService, GetDrainingNodes, max_active_rpcs_per_handler_)
  RPC_SERVICE_HANDLER(
      NodeResourceInfoGcsService, GetAllResourceUsage, max_active_rpcs_per_handler_)
}

void InternalPubSubGrpcService::InitServerCallFactories(
    const std::unique_ptr<grpc::ServerCompletionQueue> &cq,
    std::vector<std::unique_ptr<ServerCallFactory>> *server_call_factories,
    const ClusterID &cluster_id) {
  RPC_SERVICE_HANDLER(InternalPubSubGcsService, GcsPublish, max_active_rpcs_per_handler_);
  RPC_SERVICE_HANDLER(
      InternalPubSubGcsService, GcsSubscriberPoll, max_active_rpcs_per_handler_);
  RPC_SERVICE_HANDLER(
      InternalPubSubGcsService, GcsSubscriberCommandBatch, max_active_rpcs_per_handler_);
}

void JobInfoGrpcService::InitServerCallFactories(
    const std::unique_ptr<grpc::ServerCompletionQueue> &cq,
    std::vector<std::unique_ptr<ServerCallFactory>> *server_call_factories,
    const ClusterID &cluster_id) {
  RPC_SERVICE_HANDLER(JobInfoGcsService, AddJob, max_active_rpcs_per_handler_)
  RPC_SERVICE_HANDLER(JobInfoGcsService, MarkJobFinished, max_active_rpcs_per_handler_)
  RPC_SERVICE_HANDLER(JobInfoGcsService, GetAllJobInfo, max_active_rpcs_per_handler_)
  RPC_SERVICE_HANDLER(JobInfoGcsService, ReportJobError, max_active_rpcs_per_handler_)
  RPC_SERVICE_HANDLER(JobInfoGcsService, GetNextJobID, max_active_rpcs_per_handler_)
}

void RuntimeEnvGrpcService::InitServerCallFactories(
    const std::unique_ptr<grpc::ServerCompletionQueue> &cq,
    std::vector<std::unique_ptr<ServerCallFactory>> *server_call_factories,
    const ClusterID &cluster_id) {
  RPC_SERVICE_HANDLER(
      RuntimeEnvGcsService, PinRuntimeEnvURI, max_active_rpcs_per_handler_)
}

void WorkerInfoGrpcService::InitServerCallFactories(
    const std::unique_ptr<grpc::ServerCompletionQueue> &cq,
    std::vector<std::unique_ptr<ServerCallFactory>> *server_call_factories,
    const ClusterID &cluster_id) {
  RPC_SERVICE_HANDLER(
      WorkerInfoGcsService, ReportWorkerFailure, max_active_rpcs_per_handler_)
  RPC_SERVICE_HANDLER(WorkerInfoGcsService, GetWorkerInfo, max_active_rpcs_per_handler_)
  RPC_SERVICE_HANDLER(
      WorkerInfoGcsService, GetAllWorkerInfo, max_active_rpcs_per_handler_)
  RPC_SERVICE_HANDLER(WorkerInfoGcsService, AddWorkerInfo, max_active_rpcs_per_handler_)
  RPC_SERVICE_HANDLER(
      WorkerInfoGcsService, UpdateWorkerDebuggerPort, max_active_rpcs_per_handler_)
  RPC_SERVICE_HANDLER(
      WorkerInfoGcsService, UpdateWorkerNumPausedThreads, max_active_rpcs_per_handler_)
}

void InternalKVGrpcService::InitServerCallFactories(
    const std::unique_ptr<grpc::ServerCompletionQueue> &cq,
    std::vector<std::unique_ptr<ServerCallFactory>> *server_call_factories,
    const ClusterID &cluster_id) {
  RPC_SERVICE_HANDLER(InternalKVGcsService, InternalKVGet, max_active_rpcs_per_handler_)
  RPC_SERVICE_HANDLER(
      InternalKVGcsService, InternalKVMultiGet, max_active_rpcs_per_handler_)
  RPC_SERVICE_HANDLER(InternalKVGcsService, InternalKVPut, max_active_rpcs_per_handler_)
  RPC_SERVICE_HANDLER(InternalKVGcsService, InternalKVDel, max_active_rpcs_per_handler_)
  RPC_SERVICE_HANDLER(
      InternalKVGcsService, InternalKVExists, max_active_rpcs_per_handler_)
  RPC_SERVICE_HANDLER(InternalKVGcsService, InternalKVKeys, max_active_rpcs_per_handler_)
  RPC_SERVICE_HANDLER(
      InternalKVGcsService, GetInternalConfig, max_active_rpcs_per_handler_)
}

void TaskInfoGrpcService::InitServerCallFactories(
    const std::unique_ptr<grpc::ServerCompletionQueue> &cq,
    std::vector<std::unique_ptr<ServerCallFactory>> *server_call_factories,
    const ClusterID &cluster_id) {
  RPC_SERVICE_HANDLER(TaskInfoGcsService, AddTaskEventData, max_active_rpcs_per_handler_)
  RPC_SERVICE_HANDLER(TaskInfoGcsService, GetTaskEvents, max_active_rpcs_per_handler_)
}

void PlacementGroupInfoGrpcService::InitServerCallFactories(
    const std::unique_ptr<grpc::ServerCompletionQueue> &cq,
    std::vector<std::unique_ptr<ServerCallFactory>> *server_call_factories,
    const ClusterID &cluster_id) {
  RPC_SERVICE_HANDLER(
      PlacementGroupInfoGcsService, CreatePlacementGroup, max_active_rpcs_per_handler_)
  RPC_SERVICE_HANDLER(
      PlacementGroupInfoGcsService, RemovePlacementGroup, max_active_rpcs_per_handler_)
  RPC_SERVICE_HANDLER(
      PlacementGroupInfoGcsService, GetPlacementGroup, max_active_rpcs_per_handler_)
  RPC_SERVICE_HANDLER(
      PlacementGroupInfoGcsService, GetNamedPlacementGroup, max_active_rpcs_per_handler_)
  RPC_SERVICE_HANDLER(
      PlacementGroupInfoGcsService, GetAllPlacementGroup, max_active_rpcs_per_handler_)
  RPC_SERVICE_HANDLER(PlacementGroupInfoGcsService,
                      WaitPlacementGroupUntilReady,
                      max_active_rpcs_per_handler_)
}

namespace autoscaler {

void AutoscalerStateGrpcService::InitServerCallFactories(
    const std::unique_ptr<grpc::ServerCompletionQueue> &cq,
    std::vector<std::unique_ptr<ServerCallFactory>> *server_call_factories,
    const ClusterID &cluster_id) {
  RPC_SERVICE_HANDLER(
      AutoscalerStateService, GetClusterResourceState, max_active_rpcs_per_handler_)
  RPC_SERVICE_HANDLER(
      AutoscalerStateService, ReportAutoscalingState, max_active_rpcs_per_handler_)
  RPC_SERVICE_HANDLER(
      AutoscalerStateService, ReportClusterConfig, max_active_rpcs_per_handler_)
  RPC_SERVICE_HANDLER(AutoscalerStateService,
                      RequestClusterResourceConstraint,
                      max_active_rpcs_per_handler_)
  RPC_SERVICE_HANDLER(
      AutoscalerStateService, GetClusterStatus, max_active_rpcs_per_handler_)
  RPC_SERVICE_HANDLER(AutoscalerStateService, DrainNode, max_active_rpcs_per_handler_)
}

}  // namespace autoscaler

namespace events {

void RayEventExportGrpcService::InitServerCallFactories(
    const std::unique_ptr<grpc::ServerCompletionQueue> &cq,
    std::vector<std::unique_ptr<ServerCallFactory>> *server_call_factories,
    const ClusterID &cluster_id) {
  RPC_SERVICE_HANDLER(RayEventExportGcsService, AddEvents, max_active_rpcs_per_handler_)
}

}  // namespace events

}  // namespace rpc
}  // namespace ray
