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

#include <list>
#include <memory>
#include <string>
#include <utility>

#include "absl/synchronization/mutex.h"
#include "ray/common/status.h"
#include "ray/core_worker_rpc_client/core_worker_client_interface.h"
#include "src/ray/protobuf/core_worker.pb.h"
#include "src/ray/protobuf/pubsub.pb.h"

namespace ray {
namespace rpc {

class FakeCoreWorkerClient : public CoreWorkerClientInterface {
 public:
  const Address &Addr() const override {
    static Address addr;
    return addr;
  }

  bool IsIdleAfterRPCs() const override { return true; }

  void PushActorTask(std::unique_ptr<PushTaskRequest> request,
                     bool skip_queue,
                     ClientCallback<PushTaskReply> &&callback) override {}

  void PushNormalTask(std::unique_ptr<PushTaskRequest> request,
                      const ClientCallback<PushTaskReply> &callback) override {
    absl::MutexLock lock(&mutex_);
    callbacks_.push_back(callback);
  }

  void NumPendingTasks(std::unique_ptr<NumPendingTasksRequest> request,
                       const ClientCallback<NumPendingTasksReply> &callback,
                       int64_t timeout_ms = -1) override {}

  void ActorCallArgWaitComplete(
      const ActorCallArgWaitCompleteRequest &request,
      const ClientCallback<ActorCallArgWaitCompleteReply> &callback) override {}

  void GetObjectStatus(GetObjectStatusRequest &&request,
                       const ClientCallback<GetObjectStatusReply> &callback) override {}

  void WaitForActorRefDeleted(
      WaitForActorRefDeletedRequest &&request,
      const ClientCallback<WaitForActorRefDeletedReply> &callback) override {}

  void UpdateObjectLocationBatch(
      UpdateObjectLocationBatchRequest &&request,
      const ClientCallback<UpdateObjectLocationBatchReply> &callback) override {}

  void GetObjectLocationsOwner(
      const GetObjectLocationsOwnerRequest &request,
      const ClientCallback<GetObjectLocationsOwnerReply> &callback) override {}

  void ReportGeneratorItemReturns(
      ReportGeneratorItemReturnsRequest &&request,
      const ClientCallback<ReportGeneratorItemReturnsReply> &callback) override {}

  void KillActor(const KillActorRequest &request,
                 const ClientCallback<KillActorReply> &callback) override {
    num_kill_actor_requests++;
  }

  void CancelTask(const CancelTaskRequest &request,
                  const ClientCallback<CancelTaskReply> &callback) override {}

  void CancelRemoteTask(CancelRemoteTaskRequest &&request,
                        const ClientCallback<CancelRemoteTaskReply> &callback) override {}

  void RegisterMutableObjectReader(
      const RegisterMutableObjectReaderRequest &request,
      const ClientCallback<RegisterMutableObjectReaderReply> &callback) override {}

  void GetCoreWorkerStats(
      const GetCoreWorkerStatsRequest &request,
      const ClientCallback<GetCoreWorkerStatsReply> &callback) override {}

  void LocalGC(const LocalGCRequest &request,
               const ClientCallback<LocalGCReply> &callback) override {}

  void DeleteObjects(const DeleteObjectsRequest &request,
                     const ClientCallback<DeleteObjectsReply> &callback) override {}

  void SpillObjects(const SpillObjectsRequest &request,
                    const ClientCallback<SpillObjectsReply> &callback) override {}

  void RestoreSpilledObjects(
      const RestoreSpilledObjectsRequest &request,
      const ClientCallback<RestoreSpilledObjectsReply> &callback) override {}

  void DeleteSpilledObjects(
      const DeleteSpilledObjectsRequest &request,
      const ClientCallback<DeleteSpilledObjectsReply> &callback) override {}

  void PlasmaObjectReady(
      const PlasmaObjectReadyRequest &request,
      const ClientCallback<PlasmaObjectReadyReply> &callback) override {}

  void RayletNotifyGCSRestart(
      const RayletNotifyGCSRestartRequest &request,
      const ClientCallback<RayletNotifyGCSRestartReply> &callback) override {}

  void Exit(const ExitRequest &request,
            const ClientCallback<ExitReply> &callback) override {}

  void AssignObjectOwner(
      const AssignObjectOwnerRequest &request,
      const ClientCallback<AssignObjectOwnerReply> &callback) override {}

  // SubscriberClientInterface methods
  void PubsubLongPolling(
      PubsubLongPollingRequest &&request,
      const ClientCallback<PubsubLongPollingReply> &callback) override {}

  void PubsubCommandBatch(
      PubsubCommandBatchRequest &&request,
      const ClientCallback<PubsubCommandBatchReply> &callback) override {}

  std::string DebugString() const override { return "FakeCoreWorkerClient"; }

  bool ReplyPushTask(Status status = Status::OK(), bool exit = false) {
    ClientCallback<PushTaskReply> callback = nullptr;
    {
      absl::MutexLock lock(&mutex_);
      if (callbacks_.size() == 0) {
        return false;
      }
      callback = callbacks_.front();
      callbacks_.pop_front();
    }
    // call the callback without the lock to avoid deadlock.
    auto reply = PushTaskReply();
    if (exit) {
      reply.set_worker_exiting(true);
    }
    callback(status, std::move(reply));
    return true;
  }

  size_t GetNumCallbacks() {
    absl::MutexLock lock(&mutex_);
    return callbacks_.size();
  }

  std::list<ClientCallback<PushTaskReply>> callbacks_ ABSL_GUARDED_BY(mutex_);
  size_t num_kill_actor_requests = 0;
  absl::Mutex mutex_;
};

}  // namespace rpc
}  // namespace ray
