// Copyright 2021 The Ray Authors.
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

#include "gmock/gmock.h"
#include "ray/core_worker_rpc_client/core_worker_client_interface.h"

namespace ray {
namespace rpc {

class MockCoreWorkerClientInterface : public CoreWorkerClientInterface {
 public:
  MOCK_METHOD(const Address &, Addr, (), (const, override));
  MOCK_METHOD(bool, IsIdleAfterRPCs, (), (const, override));
  MOCK_METHOD(void,
              PushActorTask,
              (std::unique_ptr<PushTaskRequest> request,
               bool skip_queue,
               ClientCallback<PushTaskReply> &&callback),
              (override));
  MOCK_METHOD(void,
              PushNormalTask,
              (std::unique_ptr<PushTaskRequest> request,
               const ClientCallback<PushTaskReply> &callback),
              (override));
  MOCK_METHOD(void,
              NumPendingTasks,
              (std::unique_ptr<NumPendingTasksRequest> request,
               const ClientCallback<NumPendingTasksReply> &callback,
               int64_t timeout_ms),
              (override));
  MOCK_METHOD(void,
              ActorCallArgWaitComplete,
              (const ActorCallArgWaitCompleteRequest &request,
               const ClientCallback<ActorCallArgWaitCompleteReply> &callback),
              (override));
  MOCK_METHOD(void,
              GetObjectStatus,
              (GetObjectStatusRequest && request,
               const ClientCallback<GetObjectStatusReply> &callback),
              (override));
  MOCK_METHOD(void,
              WaitForActorRefDeleted,
              (const WaitForActorRefDeletedRequest &request,
               const ClientCallback<WaitForActorRefDeletedReply> &callback),
              (override));
  MOCK_METHOD(void,
              PubsubLongPolling,
              (PubsubLongPollingRequest && request,
               const ClientCallback<PubsubLongPollingReply> &callback),
              (override));
  MOCK_METHOD(void,
              PubsubCommandBatch,
              (PubsubCommandBatchRequest && request,
               const ClientCallback<PubsubCommandBatchReply> &callback),
              (override));
  MOCK_METHOD(void,
              UpdateObjectLocationBatch,
              (UpdateObjectLocationBatchRequest && request,
               const ClientCallback<UpdateObjectLocationBatchReply> &callback),
              (override));
  MOCK_METHOD(void,
              GetObjectLocationsOwner,
              (const GetObjectLocationsOwnerRequest &request,
               const ClientCallback<GetObjectLocationsOwnerReply> &callback),
              (override));
  MOCK_METHOD(void,
              KillActor,
              (const KillActorRequest &request,
               const ClientCallback<KillActorReply> &callback),
              (override));
  MOCK_METHOD(void,
              CancelTask,
              (const CancelTaskRequest &request,
               const ClientCallback<CancelTaskReply> &callback),
              (override));
  MOCK_METHOD(void,
              RemoteCancelTask,
              (const RemoteCancelTaskRequest &request,
               const ClientCallback<RemoteCancelTaskReply> &callback),
              (override));
  MOCK_METHOD(void,
              GetCoreWorkerStats,
              (const GetCoreWorkerStatsRequest &request,
               const ClientCallback<GetCoreWorkerStatsReply> &callback),
              (override));
  MOCK_METHOD(void,
              LocalGC,
              (const LocalGCRequest &request,
               const ClientCallback<LocalGCReply> &callback),
              (override));
  MOCK_METHOD(void,
              SpillObjects,
              (const SpillObjectsRequest &request,
               const ClientCallback<SpillObjectsReply> &callback),
              (override));
  MOCK_METHOD(void,
              RestoreSpilledObjects,
              (const RestoreSpilledObjectsRequest &request,
               const ClientCallback<RestoreSpilledObjectsReply> &callback),
              (override));
  MOCK_METHOD(void,
              DeleteSpilledObjects,
              (const DeleteSpilledObjectsRequest &request,
               const ClientCallback<DeleteSpilledObjectsReply> &callback),
              (override));
  MOCK_METHOD(void,
              PlasmaObjectReady,
              (const PlasmaObjectReadyRequest &request,
               const ClientCallback<PlasmaObjectReadyReply> &callback),
              (override));
  MOCK_METHOD(void,
              Exit,
              (const ExitRequest &request, const ClientCallback<ExitReply> &callback),
              (override));
  MOCK_METHOD(void,
              AssignObjectOwner,
              (const AssignObjectOwnerRequest &request,
               const ClientCallback<AssignObjectOwnerReply> &callback),
              (override));
  MOCK_METHOD(void,
              ReportGeneratorItemReturns,
              (ReportGeneratorItemReturnsRequest && request,
               const ClientCallback<ReportGeneratorItemReturnsReply> &callback),
              (override));
  MOCK_METHOD(void,
              RegisterMutableObjectReader,
              (const RegisterMutableObjectReaderRequest &request,
               const ClientCallback<RegisterMutableObjectReaderReply> &callback),
              (override));
  MOCK_METHOD(void,
              DeleteObjects,
              (const DeleteObjectsRequest &request,
               const ClientCallback<DeleteObjectsReply> &callback),
              (override));
  MOCK_METHOD(void,
              RayletNotifyGCSRestart,
              (const RayletNotifyGCSRestartRequest &request,
               const ClientCallback<RayletNotifyGCSRestartReply> &callback),
              (override));
  MOCK_METHOD(std::string, DebugString, (), (const, override));
};

class MockCoreWorkerClientConfigurableRunningTasks
    : public MockCoreWorkerClientInterface {
 public:
  explicit MockCoreWorkerClientConfigurableRunningTasks(int num_running_tasks)
      : num_running_tasks_(num_running_tasks) {}

  void NumPendingTasks(std::unique_ptr<NumPendingTasksRequest> request,
                       const ClientCallback<NumPendingTasksReply> &callback,
                       int64_t timeout_ms = -1) override {
    NumPendingTasksReply reply;
    reply.set_num_pending_tasks(num_running_tasks_);
    callback(Status::OK(), std::move(reply));
  }

 private:
  int num_running_tasks_;
};

}  // namespace rpc
}  // namespace ray
