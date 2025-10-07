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

#include <string>

#include "ray/pubsub/subscriber_interface.h"
#include "ray/rpc/rpc_callback_types.h"
#include "src/ray/protobuf/common.pb.h"
#include "src/ray/protobuf/core_worker.pb.h"
#include "src/ray/protobuf/pubsub.pb.h"

namespace ray {
namespace rpc {

class CoreWorkerClientInterface : public pubsub::SubscriberClientInterface {
 public:
  virtual const rpc::Address &Addr() const = 0;

  /// Returns true if the grpc channel is idle and there are no pending requests
  /// after at least one RPC call is made.
  virtual bool IsIdleAfterRPCs() const = 0;

  // Actor / task submission RPCs
  virtual void PushActorTask(std::unique_ptr<PushTaskRequest> request,
                             bool skip_queue,
                             ClientCallback<PushTaskReply> &&callback) = 0;

  virtual void PushNormalTask(std::unique_ptr<PushTaskRequest> request,
                              const ClientCallback<PushTaskReply> &callback) = 0;

  virtual void NumPendingTasks(std::unique_ptr<NumPendingTasksRequest> request,
                               const ClientCallback<NumPendingTasksReply> &callback,
                               int64_t timeout_ms = -1) = 0;

  virtual void ActorCallArgWaitComplete(
      const ActorCallArgWaitCompleteRequest &request,
      const ClientCallback<ActorCallArgWaitCompleteReply> &callback) = 0;

  virtual void GetObjectStatus(GetObjectStatusRequest &&request,
                               const ClientCallback<GetObjectStatusReply> &callback) = 0;

  virtual void WaitForActorRefDeleted(
      const WaitForActorRefDeletedRequest &request,
      const ClientCallback<WaitForActorRefDeletedReply> &callback) = 0;

  // Object location / ownership RPCs
  virtual void UpdateObjectLocationBatch(
      UpdateObjectLocationBatchRequest &&request,
      const ClientCallback<UpdateObjectLocationBatchReply> &callback) = 0;

  virtual void GetObjectLocationsOwner(
      const GetObjectLocationsOwnerRequest &request,
      const ClientCallback<GetObjectLocationsOwnerReply> &callback) = 0;

  virtual void ReportGeneratorItemReturns(
      ReportGeneratorItemReturnsRequest &&request,
      const ClientCallback<ReportGeneratorItemReturnsReply> &callback) = 0;

  // Lifecycle / control RPCs
  virtual void KillActor(const KillActorRequest &request,
                         const ClientCallback<KillActorReply> &callback) = 0;

  virtual void CancelTask(const CancelTaskRequest &request,
                          const ClientCallback<CancelTaskReply> &callback) = 0;

  virtual void RemoteCancelTask(
      const RemoteCancelTaskRequest &request,
      const ClientCallback<RemoteCancelTaskReply> &callback) = 0;

  virtual void RegisterMutableObjectReader(
      const RegisterMutableObjectReaderRequest &request,
      const ClientCallback<RegisterMutableObjectReaderReply> &callback) = 0;

  virtual void GetCoreWorkerStats(
      const GetCoreWorkerStatsRequest &request,
      const ClientCallback<GetCoreWorkerStatsReply> &callback) = 0;

  virtual void LocalGC(const LocalGCRequest &request,
                       const ClientCallback<LocalGCReply> &callback) = 0;

  virtual void DeleteObjects(const DeleteObjectsRequest &request,
                             const ClientCallback<DeleteObjectsReply> &callback) = 0;

  virtual void SpillObjects(const SpillObjectsRequest &request,
                            const ClientCallback<SpillObjectsReply> &callback) = 0;

  virtual void RestoreSpilledObjects(
      const RestoreSpilledObjectsRequest &request,
      const ClientCallback<RestoreSpilledObjectsReply> &callback) = 0;

  virtual void DeleteSpilledObjects(
      const DeleteSpilledObjectsRequest &request,
      const ClientCallback<DeleteSpilledObjectsReply> &callback) = 0;

  virtual void PlasmaObjectReady(
      const PlasmaObjectReadyRequest &request,
      const ClientCallback<PlasmaObjectReadyReply> &callback) = 0;

  virtual void RayletNotifyGCSRestart(
      const RayletNotifyGCSRestartRequest &request,
      const ClientCallback<RayletNotifyGCSRestartReply> &callback) = 0;

  virtual void Exit(const ExitRequest &request,
                    const ClientCallback<ExitReply> &callback) = 0;

  virtual void AssignObjectOwner(
      const AssignObjectOwnerRequest &request,
      const ClientCallback<AssignObjectOwnerReply> &callback) = 0;

  virtual std::string DebugString() const = 0;
};

}  // namespace rpc
}  // namespace ray
