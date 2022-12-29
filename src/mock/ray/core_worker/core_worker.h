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
#include "mock/ray/gcs/gcs_client/gcs_client.h"
namespace ray {
namespace core {

class MockCoreWorkerOptions : public CoreWorkerOptions {
 public:
};

}  // namespace core
}  // namespace ray

namespace ray {
namespace core {

class MockCoreWorkerProcess : public CoreWorkerProcess {
 public:
};

}  // namespace core
}  // namespace ray

namespace ray {
namespace core {

class MockCoreWorker : public CoreWorker {
 public:
  MOCK_METHOD(void,
              HandlePushTask,
              (rpc::PushTaskRequest request,
               rpc::PushTaskReply *reply,
               rpc::SendReplyCallback send_reply_callback),
              (override));
  MOCK_METHOD(void,
              HandleDirectActorCallArgWaitComplete,
              (rpc::DirectActorCallArgWaitCompleteRequest request,
               rpc::DirectActorCallArgWaitCompleteReply *reply,
               rpc::SendReplyCallback send_reply_callback),
              (override));
  MOCK_METHOD(void,
              HandleGetObjectStatus,
              (rpc::GetObjectStatusRequest request,
               rpc::GetObjectStatusReply *reply,
               rpc::SendReplyCallback send_reply_callback),
              (override));
  MOCK_METHOD(void,
              HandleWaitForActorOutOfScope,
              (rpc::WaitForActorOutOfScopeRequest request,
               rpc::WaitForActorOutOfScopeReply *reply,
               rpc::SendReplyCallback send_reply_callback),
              (override));
  MOCK_METHOD(void,
              HandlePubsubLongPolling,
              (rpc::PubsubLongPollingRequest request,
               rpc::PubsubLongPollingReply *reply,
               rpc::SendReplyCallback send_reply_callback),
              (override));
  MOCK_METHOD(void,
              HandlePubsubCommandBatch,
              (rpc::PubsubCommandBatchRequest request,
               rpc::PubsubCommandBatchReply *reply,
               rpc::SendReplyCallback send_reply_callback),
              (override));
  MOCK_METHOD(void,
              HandleAddObjectLocationOwner,
              (rpc::AddObjectLocationOwnerRequest request,
               rpc::AddObjectLocationOwnerReply *reply,
               rpc::SendReplyCallback send_reply_callback),
              (override));
  MOCK_METHOD(void,
              HandleRemoveObjectLocationOwner,
              (rpc::RemoveObjectLocationOwnerRequest request,
               rpc::RemoveObjectLocationOwnerReply *reply,
               rpc::SendReplyCallback send_reply_callback),
              (override));
  MOCK_METHOD(void,
              HandleGetObjectLocationsOwner,
              (rpc::GetObjectLocationsOwnerRequest request,
               rpc::GetObjectLocationsOwnerReply *reply,
               rpc::SendReplyCallback send_reply_callback),
              (override));
  MOCK_METHOD(void,
              HandleKillActor,
              (rpc::KillActorRequest request,
               rpc::KillActorReply *reply,
               rpc::SendReplyCallback send_reply_callback),
              (override));
  MOCK_METHOD(void,
              HandleCancelTask,
              (rpc::CancelTaskRequest request,
               rpc::CancelTaskReply *reply,
               rpc::SendReplyCallback send_reply_callback),
              (override));
  MOCK_METHOD(void,
              HandleRemoteCancelTask,
              (rpc::RemoteCancelTaskRequest request,
               rpc::RemoteCancelTaskReply *reply,
               rpc::SendReplyCallback send_reply_callback),
              (override));
  MOCK_METHOD(void,
              HandlePlasmaObjectReady,
              (rpc::PlasmaObjectReadyRequest request,
               rpc::PlasmaObjectReadyReply *reply,
               rpc::SendReplyCallback send_reply_callback),
              (override));
  MOCK_METHOD(void,
              HandleGetCoreWorkerStats,
              (rpc::GetCoreWorkerStatsRequest request,
               rpc::GetCoreWorkerStatsReply *reply,
               rpc::SendReplyCallback send_reply_callback),
              (override));
  MOCK_METHOD(void,
              HandleLocalGC,
              (rpc::LocalGCRequest request,
               rpc::LocalGCReply *reply,
               rpc::SendReplyCallback send_reply_callback),
              (override));
  MOCK_METHOD(void,
              HandleRunOnUtilWorker,
              (rpc::RunOnUtilWorkerRequest request,
               rpc::RunOnUtilWorkerReply *reply,
               rpc::SendReplyCallback send_reply_callback),
              (override));
  MOCK_METHOD(void,
              HandleSpillObjects,
              (rpc::SpillObjectsRequest request,
               rpc::SpillObjectsReply *reply,
               rpc::SendReplyCallback send_reply_callback),
              (override));
  MOCK_METHOD(void,
              HandleRestoreSpilledObjects,
              (rpc::RestoreSpilledObjectsRequest request,
               rpc::RestoreSpilledObjectsReply *reply,
               rpc::SendReplyCallback send_reply_callback),
              (override));
  MOCK_METHOD(void,
              HandleDeleteSpilledObjects,
              (rpc::DeleteSpilledObjectsRequest request,
               rpc::DeleteSpilledObjectsReply *reply,
               rpc::SendReplyCallback send_reply_callback),
              (override));
  MOCK_METHOD(void,
              HandleExit,
              (rpc::ExitRequest request,
               rpc::ExitReply *reply,
               rpc::SendReplyCallback send_reply_callback),
              (override));
  MOCK_METHOD(void,
              HandleAssignObjectOwner,
              (rpc::AssignObjectOwnerRequest request,
               rpc::AssignObjectOwnerReply *reply,
               rpc::SendReplyCallback send_reply_callback),
              (override));
};

}  // namespace core
}  // namespace ray
