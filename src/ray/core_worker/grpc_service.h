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

/* XXX */

/// The set of gRPC handlers and their associated level of concurrency. If you want to
/// add a new call to the worker gRPC server, do the following:
/// 1) Add the rpc to the CoreWorkerService in core_worker.proto, e.g., "ExampleCall"
/// 2) Add a new macro to RAY_CORE_WORKER_DECLARE_RPC_HANDLERS
///    in core_worker_server.h,
//     e.g. "DECLARE_VOID_RPC_SERVICE_HANDLER_METHOD(ExampleCall)"
/// 3) Add a new macro to RAY_CORE_WORKER_RPC_HANDLERS in core_worker_server.h, e.g.
///    "RPC_SERVICE_HANDLER(CoreWorkerService, ExampleCall, 1)"
/// 4) Add a method to the CoreWorker class below: "CoreWorker::HandleExampleCall"

#pragma once

#include <memory>
#include <vector>

#include "ray/common/asio/instrumented_io_context.h"
#include "ray/rpc/grpc_server.h"
#include "ray/rpc/server_call.h"
#include "src/ray/protobuf/core_worker.grpc.pb.h"
#include "src/ray/protobuf/core_worker.pb.h"

namespace ray {

class CoreWorker;

namespace rpc {
/// TODO(vitsai): Remove this when auth is implemented for node manager
#define RAY_CORE_WORKER_RPC_SERVICE_HANDLER(METHOD)        \
  RPC_SERVICE_HANDLER_CUSTOM_AUTH_SERVER_METRICS_DISABLED( \
      CoreWorkerService, METHOD, -1, AuthType::NO_AUTH)

/// Disable gRPC server metrics since it incurs too high cardinality.
#define RAY_CORE_WORKER_RPC_HANDLERS                               \
  RAY_CORE_WORKER_RPC_SERVICE_HANDLER(PushTask)                    \
  RAY_CORE_WORKER_RPC_SERVICE_HANDLER(ActorCallArgWaitComplete)    \
  RAY_CORE_WORKER_RPC_SERVICE_HANDLER(RayletNotifyGCSRestart)      \
  RAY_CORE_WORKER_RPC_SERVICE_HANDLER(GetObjectStatus)             \
  RAY_CORE_WORKER_RPC_SERVICE_HANDLER(WaitForActorRefDeleted)      \
  RAY_CORE_WORKER_RPC_SERVICE_HANDLER(PubsubLongPolling)           \
  RAY_CORE_WORKER_RPC_SERVICE_HANDLER(PubsubCommandBatch)          \
  RAY_CORE_WORKER_RPC_SERVICE_HANDLER(UpdateObjectLocationBatch)   \
  RAY_CORE_WORKER_RPC_SERVICE_HANDLER(GetObjectLocationsOwner)     \
  RAY_CORE_WORKER_RPC_SERVICE_HANDLER(ReportGeneratorItemReturns)  \
  RAY_CORE_WORKER_RPC_SERVICE_HANDLER(KillActor)                   \
  RAY_CORE_WORKER_RPC_SERVICE_HANDLER(CancelTask)                  \
  RAY_CORE_WORKER_RPC_SERVICE_HANDLER(RemoteCancelTask)            \
  RAY_CORE_WORKER_RPC_SERVICE_HANDLER(RegisterMutableObjectReader) \
  RAY_CORE_WORKER_RPC_SERVICE_HANDLER(GetCoreWorkerStats)          \
  RAY_CORE_WORKER_RPC_SERVICE_HANDLER(LocalGC)                     \
  RAY_CORE_WORKER_RPC_SERVICE_HANDLER(DeleteObjects)               \
  RAY_CORE_WORKER_RPC_SERVICE_HANDLER(SpillObjects)                \
  RAY_CORE_WORKER_RPC_SERVICE_HANDLER(RestoreSpilledObjects)       \
  RAY_CORE_WORKER_RPC_SERVICE_HANDLER(DeleteSpilledObjects)        \
  RAY_CORE_WORKER_RPC_SERVICE_HANDLER(PlasmaObjectReady)           \
  RAY_CORE_WORKER_RPC_SERVICE_HANDLER(Exit)                        \
  RAY_CORE_WORKER_RPC_SERVICE_HANDLER(AssignObjectOwner)           \
  RAY_CORE_WORKER_RPC_SERVICE_HANDLER(NumPendingTasks)             \
  RAY_CORE_WORKER_RPC_SERVICE_HANDLER(FreeActorObject)

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

  virtual void HandleFreeActorObject(FreeActorObjectRequest request,
                                     FreeActorObjectReply *reply,
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
