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
 * This file defines the gRPC service handlers for the core worker server.
 *
 * core_worker_process should be the only user of this target. If other classes need the
 * CoreWorkerInterface in the future, split it into its own target that does not include
 * the heavyweight gRPC headers..
 *
 * To add a new RPC handler:
 *   - Update core_worker.proto.
 *   - Add a virtual method to CoreWorkerService.
 *   - Initialize the handler for the method in InitServerCallFactories.
 *   - Implement the method in core_worker.
 */

#pragma once

#include <memory>
#include <vector>

#include "ray/common/asio/instrumented_io_context.h"
#include "ray/rpc/grpc_server.h"
#include "ray/rpc/rpc_callback_types.h"
#include "src/ray/protobuf/core_worker.grpc.pb.h"
#include "src/ray/protobuf/core_worker.pb.h"

namespace ray {
namespace rpc {

class CoreWorkerServiceHandler : public DelayedServiceHandler {
 public:
  /// Blocks until the service is ready to serve RPCs.
  virtual void WaitUntilInitialized() = 0;

  virtual void HandlePushTask(PushTaskRequest request,
                              PushTaskReply *reply,
                              SendReplyCallback send_reply_callback) = 0;

  virtual void HandleActorCallArgWaitComplete(ActorCallArgWaitCompleteRequest request,
                                              ActorCallArgWaitCompleteReply *reply,
                                              SendReplyCallback send_reply_callback) = 0;

  virtual void HandleRayletNotifyGCSRestart(RayletNotifyGCSRestartRequest request,
                                            RayletNotifyGCSRestartReply *reply,
                                            SendReplyCallback send_reply_callback) = 0;

  virtual void HandleGetObjectStatus(GetObjectStatusRequest request,
                                     GetObjectStatusReply *reply,
                                     SendReplyCallback send_reply_callback) = 0;

  virtual void HandleWaitForActorRefDeleted(WaitForActorRefDeletedRequest request,
                                            WaitForActorRefDeletedReply *reply,
                                            SendReplyCallback send_reply_callback) = 0;

  virtual void HandlePubsubLongPolling(PubsubLongPollingRequest request,
                                       PubsubLongPollingReply *reply,
                                       SendReplyCallback send_reply_callback) = 0;

  virtual void HandlePubsubCommandBatch(PubsubCommandBatchRequest request,
                                        PubsubCommandBatchReply *reply,
                                        SendReplyCallback send_reply_callback) = 0;

  virtual void HandleUpdateObjectLocationBatch(UpdateObjectLocationBatchRequest request,
                                               UpdateObjectLocationBatchReply *reply,
                                               SendReplyCallback send_reply_callback) = 0;
  virtual void HandleGetObjectLocationsOwner(GetObjectLocationsOwnerRequest request,
                                             GetObjectLocationsOwnerReply *reply,
                                             SendReplyCallback send_reply_callback) = 0;

  virtual void HandleReportGeneratorItemReturns(
      ReportGeneratorItemReturnsRequest request,
      ReportGeneratorItemReturnsReply *reply,
      SendReplyCallback send_reply_callback) = 0;

  virtual void HandleKillActor(KillActorRequest request,
                               KillActorReply *reply,
                               SendReplyCallback send_reply_callback) = 0;

  virtual void HandleCancelTask(CancelTaskRequest request,
                                CancelTaskReply *reply,
                                SendReplyCallback send_reply_callback) = 0;

  virtual void HandleRemoteCancelTask(RemoteCancelTaskRequest request,
                                      RemoteCancelTaskReply *reply,
                                      SendReplyCallback send_reply_callback) = 0;

  virtual void HandleRegisterMutableObjectReader(
      RegisterMutableObjectReaderRequest request,
      RegisterMutableObjectReaderReply *reply,
      SendReplyCallback send_reply_callback) = 0;

  virtual void HandleGetCoreWorkerStats(GetCoreWorkerStatsRequest request,
                                        GetCoreWorkerStatsReply *reply,
                                        SendReplyCallback send_reply_callback) = 0;

  virtual void HandleLocalGC(LocalGCRequest request,
                             LocalGCReply *reply,
                             SendReplyCallback send_reply_callback) = 0;

  virtual void HandleDeleteObjects(DeleteObjectsRequest request,
                                   DeleteObjectsReply *reply,
                                   SendReplyCallback send_reply_callback) = 0;

  virtual void HandleSpillObjects(SpillObjectsRequest request,
                                  SpillObjectsReply *reply,
                                  SendReplyCallback send_reply_callback) = 0;

  virtual void HandleRestoreSpilledObjects(RestoreSpilledObjectsRequest request,
                                           RestoreSpilledObjectsReply *reply,
                                           SendReplyCallback send_reply_callback) = 0;

  virtual void HandleDeleteSpilledObjects(DeleteSpilledObjectsRequest request,
                                          DeleteSpilledObjectsReply *reply,
                                          SendReplyCallback send_reply_callback) = 0;

  virtual void HandlePlasmaObjectReady(PlasmaObjectReadyRequest request,
                                       PlasmaObjectReadyReply *reply,
                                       SendReplyCallback send_reply_callback) = 0;

  virtual void HandleExit(ExitRequest request,
                          ExitReply *reply,
                          SendReplyCallback send_reply_callback) = 0;

  virtual void HandleAssignObjectOwner(AssignObjectOwnerRequest request,
                                       AssignObjectOwnerReply *reply,
                                       SendReplyCallback send_reply_callback) = 0;

  virtual void HandleNumPendingTasks(NumPendingTasksRequest request,
                                     NumPendingTasksReply *reply,
                                     SendReplyCallback send_reply_callback) = 0;
};

class CoreWorkerGrpcService : public GrpcService {
 public:
  CoreWorkerGrpcService(instrumented_io_context &main_service,
                        CoreWorkerServiceHandler &service_handler,
                        int64_t max_active_rpcs_per_handler)
      : GrpcService(main_service),
        service_handler_(service_handler),
        max_active_rpcs_per_handler_(max_active_rpcs_per_handler) {}

 protected:
  grpc::Service &GetGrpcService() override { return service_; }

  void InitServerCallFactories(
      const std::unique_ptr<grpc::ServerCompletionQueue> &cq,
      std::vector<std::unique_ptr<ServerCallFactory>> *server_call_factories,
      const ClusterID &cluster_id) override;

 private:
  CoreWorkerService::AsyncService service_;
  CoreWorkerServiceHandler &service_handler_;
  int64_t max_active_rpcs_per_handler_;
};

}  // namespace rpc
}  // namespace ray
