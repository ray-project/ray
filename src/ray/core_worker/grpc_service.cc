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

#include "ray/core_worker/grpc_service.h"

#include <memory>
#include <vector>

namespace ray {
namespace rpc {

void CoreWorkerGrpcService::InitServerCallFactories(
    const std::unique_ptr<grpc::ServerCompletionQueue> &cq,
    std::vector<std::unique_ptr<ServerCallFactory>> *server_call_factories,
    const ClusterID &cluster_id) {
  /// TODO(vitsai): Remove this when auth is implemented for node manager.
  /// Disable gRPC server metrics since it incurs too high cardinality.
  RPC_SERVICE_HANDLER_CUSTOM_AUTH_SERVER_METRICS_DISABLED(
      CoreWorkerService, PushTask, max_active_rpcs_per_handler_, AuthType::NO_AUTH);
  RPC_SERVICE_HANDLER_CUSTOM_AUTH_SERVER_METRICS_DISABLED(CoreWorkerService,
                                                          ActorCallArgWaitComplete,
                                                          max_active_rpcs_per_handler_,
                                                          AuthType::NO_AUTH);
  RPC_SERVICE_HANDLER_CUSTOM_AUTH_SERVER_METRICS_DISABLED(CoreWorkerService,
                                                          RayletNotifyGCSRestart,
                                                          max_active_rpcs_per_handler_,
                                                          AuthType::NO_AUTH);
  RPC_SERVICE_HANDLER_CUSTOM_AUTH_SERVER_METRICS_DISABLED(CoreWorkerService,
                                                          GetObjectStatus,
                                                          max_active_rpcs_per_handler_,
                                                          AuthType::NO_AUTH);
  RPC_SERVICE_HANDLER_CUSTOM_AUTH_SERVER_METRICS_DISABLED(CoreWorkerService,
                                                          WaitForActorRefDeleted,
                                                          max_active_rpcs_per_handler_,
                                                          AuthType::NO_AUTH);
  RPC_SERVICE_HANDLER_CUSTOM_AUTH_SERVER_METRICS_DISABLED(CoreWorkerService,
                                                          PubsubLongPolling,
                                                          max_active_rpcs_per_handler_,
                                                          AuthType::NO_AUTH);
  RPC_SERVICE_HANDLER_CUSTOM_AUTH_SERVER_METRICS_DISABLED(CoreWorkerService,
                                                          PubsubCommandBatch,
                                                          max_active_rpcs_per_handler_,
                                                          AuthType::NO_AUTH);
  RPC_SERVICE_HANDLER_CUSTOM_AUTH_SERVER_METRICS_DISABLED(CoreWorkerService,
                                                          UpdateObjectLocationBatch,
                                                          max_active_rpcs_per_handler_,
                                                          AuthType::NO_AUTH);
  RPC_SERVICE_HANDLER_CUSTOM_AUTH_SERVER_METRICS_DISABLED(CoreWorkerService,
                                                          GetObjectLocationsOwner,
                                                          max_active_rpcs_per_handler_,
                                                          AuthType::NO_AUTH);
  RPC_SERVICE_HANDLER_CUSTOM_AUTH_SERVER_METRICS_DISABLED(CoreWorkerService,
                                                          ReportGeneratorItemReturns,
                                                          max_active_rpcs_per_handler_,
                                                          AuthType::NO_AUTH);
  RPC_SERVICE_HANDLER_CUSTOM_AUTH_SERVER_METRICS_DISABLED(
      CoreWorkerService, KillActor, max_active_rpcs_per_handler_, AuthType::NO_AUTH);
  RPC_SERVICE_HANDLER_CUSTOM_AUTH_SERVER_METRICS_DISABLED(
      CoreWorkerService, CancelTask, max_active_rpcs_per_handler_, AuthType::NO_AUTH);
  RPC_SERVICE_HANDLER_CUSTOM_AUTH_SERVER_METRICS_DISABLED(CoreWorkerService,
                                                          RemoteCancelTask,
                                                          max_active_rpcs_per_handler_,
                                                          AuthType::NO_AUTH);
  RPC_SERVICE_HANDLER_CUSTOM_AUTH_SERVER_METRICS_DISABLED(CoreWorkerService,
                                                          RegisterMutableObjectReader,
                                                          max_active_rpcs_per_handler_,
                                                          AuthType::NO_AUTH);
  RPC_SERVICE_HANDLER_CUSTOM_AUTH_SERVER_METRICS_DISABLED(CoreWorkerService,
                                                          GetCoreWorkerStats,
                                                          max_active_rpcs_per_handler_,
                                                          AuthType::NO_AUTH);
  RPC_SERVICE_HANDLER_CUSTOM_AUTH_SERVER_METRICS_DISABLED(
      CoreWorkerService, LocalGC, max_active_rpcs_per_handler_, AuthType::NO_AUTH);
  RPC_SERVICE_HANDLER_CUSTOM_AUTH_SERVER_METRICS_DISABLED(
      CoreWorkerService, DeleteObjects, max_active_rpcs_per_handler_, AuthType::NO_AUTH);
  RPC_SERVICE_HANDLER_CUSTOM_AUTH_SERVER_METRICS_DISABLED(
      CoreWorkerService, SpillObjects, max_active_rpcs_per_handler_, AuthType::NO_AUTH);
  RPC_SERVICE_HANDLER_CUSTOM_AUTH_SERVER_METRICS_DISABLED(CoreWorkerService,
                                                          RestoreSpilledObjects,
                                                          max_active_rpcs_per_handler_,
                                                          AuthType::NO_AUTH);
  RPC_SERVICE_HANDLER_CUSTOM_AUTH_SERVER_METRICS_DISABLED(CoreWorkerService,
                                                          DeleteSpilledObjects,
                                                          max_active_rpcs_per_handler_,
                                                          AuthType::NO_AUTH);
  RPC_SERVICE_HANDLER_CUSTOM_AUTH_SERVER_METRICS_DISABLED(CoreWorkerService,
                                                          PlasmaObjectReady,
                                                          max_active_rpcs_per_handler_,
                                                          AuthType::NO_AUTH);
  RPC_SERVICE_HANDLER_CUSTOM_AUTH_SERVER_METRICS_DISABLED(
      CoreWorkerService, Exit, max_active_rpcs_per_handler_, AuthType::NO_AUTH);
  RPC_SERVICE_HANDLER_CUSTOM_AUTH_SERVER_METRICS_DISABLED(CoreWorkerService,
                                                          AssignObjectOwner,
                                                          max_active_rpcs_per_handler_,
                                                          AuthType::NO_AUTH);
  RPC_SERVICE_HANDLER_CUSTOM_AUTH_SERVER_METRICS_DISABLED(CoreWorkerService,
                                                          NumPendingTasks,
                                                          max_active_rpcs_per_handler_,
                                                          AuthType::NO_AUTH);
}

}  // namespace rpc
}  // namespace ray
