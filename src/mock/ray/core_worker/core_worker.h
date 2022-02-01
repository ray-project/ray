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
  MOCK_METHOD(void, HandlePushTask,
              (const rpc::PushTaskRequest &request, rpc::PushTaskReply *reply,
               rpc::SendReplyCallback send_reply_callback),
              (override));
  MOCK_METHOD(void, HandleDirectActorCallArgWaitComplete,
              (const rpc::DirectActorCallArgWaitCompleteRequest &request,
               rpc::DirectActorCallArgWaitCompleteReply *reply,
               rpc::SendReplyCallback send_reply_callback),
              (override));
  MOCK_METHOD(void, HandleGetObjectStatus,
              (const rpc::GetObjectStatusRequest &request,
               rpc::GetObjectStatusReply *reply,
               rpc::SendReplyCallback send_reply_callback),
              (override));
  MOCK_METHOD(void, HandleWaitForActorOutOfScope,
              (const rpc::WaitForActorOutOfScopeRequest &request,
               rpc::WaitForActorOutOfScopeReply *reply,
               rpc::SendReplyCallback send_reply_callback),
              (override));
  MOCK_METHOD(void, HandlePubsubLongPolling,
              (const rpc::PubsubLongPollingRequest &request,
               rpc::PubsubLongPollingReply *reply,
               rpc::SendReplyCallback send_reply_callback),
              (override));
  MOCK_METHOD(void, HandlePubsubCommandBatch,
              (const rpc::PubsubCommandBatchRequest &request,
               rpc::PubsubCommandBatchReply *reply,
               rpc::SendReplyCallback send_reply_callback),
              (override));
  MOCK_METHOD(void, HandleAddObjectLocationOwner,
              (const rpc::AddObjectLocationOwnerRequest &request,
               rpc::AddObjectLocationOwnerReply *reply,
               rpc::SendReplyCallback send_reply_callback),
              (override));
  MOCK_METHOD(void, HandleRemoveObjectLocationOwner,
              (const rpc::RemoveObjectLocationOwnerRequest &request,
               rpc::RemoveObjectLocationOwnerReply *reply,
               rpc::SendReplyCallback send_reply_callback),
              (override));
  MOCK_METHOD(void, HandleGetObjectLocationsOwner,
              (const rpc::GetObjectLocationsOwnerRequest &request,
               rpc::GetObjectLocationsOwnerReply *reply,
               rpc::SendReplyCallback send_reply_callback),
              (override));
  MOCK_METHOD(void, HandleKillActor,
              (const rpc::KillActorRequest &request, rpc::KillActorReply *reply,
               rpc::SendReplyCallback send_reply_callback),
              (override));
  MOCK_METHOD(void, HandleCancelTask,
              (const rpc::CancelTaskRequest &request, rpc::CancelTaskReply *reply,
               rpc::SendReplyCallback send_reply_callback),
              (override));
  MOCK_METHOD(void, HandleRemoteCancelTask,
              (const rpc::RemoteCancelTaskRequest &request,
               rpc::RemoteCancelTaskReply *reply,
               rpc::SendReplyCallback send_reply_callback),
              (override));
  MOCK_METHOD(void, HandlePlasmaObjectReady,
              (const rpc::PlasmaObjectReadyRequest &request,
               rpc::PlasmaObjectReadyReply *reply,
               rpc::SendReplyCallback send_reply_callback),
              (override));
  MOCK_METHOD(void, HandleGetCoreWorkerStats,
              (const rpc::GetCoreWorkerStatsRequest &request,
               rpc::GetCoreWorkerStatsReply *reply,
               rpc::SendReplyCallback send_reply_callback),
              (override));
  MOCK_METHOD(void, HandleLocalGC,
              (const rpc::LocalGCRequest &request, rpc::LocalGCReply *reply,
               rpc::SendReplyCallback send_reply_callback),
              (override));
  MOCK_METHOD(void, HandleRunOnUtilWorker,
              (const rpc::RunOnUtilWorkerRequest &request,
               rpc::RunOnUtilWorkerReply *reply,
               rpc::SendReplyCallback send_reply_callback),
              (override));
  MOCK_METHOD(void, HandleSpillObjects,
              (const rpc::SpillObjectsRequest &request, rpc::SpillObjectsReply *reply,
               rpc::SendReplyCallback send_reply_callback),
              (override));
  MOCK_METHOD(void, HandleAddSpilledUrl,
              (const rpc::AddSpilledUrlRequest &request, rpc::AddSpilledUrlReply *reply,
               rpc::SendReplyCallback send_reply_callback),
              (override));
  MOCK_METHOD(void, HandleRestoreSpilledObjects,
              (const rpc::RestoreSpilledObjectsRequest &request,
               rpc::RestoreSpilledObjectsReply *reply,
               rpc::SendReplyCallback send_reply_callback),
              (override));
  MOCK_METHOD(void, HandleDeleteSpilledObjects,
              (const rpc::DeleteSpilledObjectsRequest &request,
               rpc::DeleteSpilledObjectsReply *reply,
               rpc::SendReplyCallback send_reply_callback),
              (override));
  MOCK_METHOD(void, HandleExit,
              (const rpc::ExitRequest &request, rpc::ExitReply *reply,
               rpc::SendReplyCallback send_reply_callback),
              (override));
  MOCK_METHOD(void, HandleAssignObjectOwner,
              (const rpc::AssignObjectOwnerRequest &request,
               rpc::AssignObjectOwnerReply *reply,
               rpc::SendReplyCallback send_reply_callback),
              (override));
};

}  // namespace core
}  // namespace ray
