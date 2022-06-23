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

namespace ray {
namespace rpc {

class MockWorkerAddress : public WorkerAddress {
 public:
};

}  // namespace rpc
}  // namespace ray

namespace ray {
namespace rpc {

class MockCoreWorkerClientInterface : public ray::pubsub::MockSubscriberClientInterface,
                                      public CoreWorkerClientInterface {
 public:
  MOCK_METHOD(const rpc::Address &, Addr, (), (const, override));
  MOCK_METHOD(void,
              PushActorTask,
              (std::unique_ptr<PushTaskRequest> request,
               bool skip_queue,
               const ClientCallback<PushTaskReply> &callback),
              (override));
  MOCK_METHOD(void,
              PushNormalTask,
              (std::unique_ptr<PushTaskRequest> request,
               const ClientCallback<PushTaskReply> &callback),
              (override));
  MOCK_METHOD(void,
              DirectActorCallArgWaitComplete,
              (const DirectActorCallArgWaitCompleteRequest &request,
               const ClientCallback<DirectActorCallArgWaitCompleteReply> &callback),
              (override));
  MOCK_METHOD(void,
              GetObjectStatus,
              (const GetObjectStatusRequest &request,
               const ClientCallback<GetObjectStatusReply> &callback),
              (override));
  MOCK_METHOD(void,
              WaitForActorOutOfScope,
              (const WaitForActorOutOfScopeRequest &request,
               const ClientCallback<WaitForActorOutOfScopeReply> &callback),
              (override));
  MOCK_METHOD(void,
              PubsubLongPolling,
              (const PubsubLongPollingRequest &request,
               const ClientCallback<PubsubLongPollingReply> &callback),
              (override));
  MOCK_METHOD(void,
              PubsubCommandBatch,
              (const PubsubCommandBatchRequest &request,
               const ClientCallback<PubsubCommandBatchReply> &callback),
              (override));
  MOCK_METHOD(void,
              UpdateObjectLocationBatch,
              (const UpdateObjectLocationBatchRequest &request,
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
  MOCK_METHOD(int64_t, ClientProcessedUpToSeqno, (), (override));
};

}  // namespace rpc
}  // namespace ray
