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
namespace raylet {

class MockNodeManagerConfig : public NodeManagerConfig {
 public:
};

}  // namespace raylet
}  // namespace ray

namespace ray {
namespace raylet {

class MockNodeManager : public NodeManager {
 public:
  MOCK_METHOD(void,
              HandleUpdateResourceUsage,
              (rpc::UpdateResourceUsageRequest request,
               rpc::UpdateResourceUsageReply *reply,
               rpc::SendReplyCallback send_reply_callback),
              (override));
  MOCK_METHOD(void,
              HandleRequestResourceReport,
              (rpc::RequestResourceReportRequest request,
               rpc::RequestResourceReportReply *reply,
               rpc::SendReplyCallback send_reply_callback),
              (override));
  MOCK_METHOD(void,
              HandleGetResourceLoad,
              (rpc::GetResourceLoadRequest request,
               rpc::GetResourceLoadReply *reply,
               rpc::SendReplyCallback send_reply_callback),
              (override));
  MOCK_METHOD(void,
              HandlePrepareBundleResources,
              (rpc::PrepareBundleResourcesRequest request,
               rpc::PrepareBundleResourcesReply *reply,
               rpc::SendReplyCallback send_reply_callback),
              (override));
  MOCK_METHOD(void,
              HandleCommitBundleResources,
              (rpc::CommitBundleResourcesRequest request,
               rpc::CommitBundleResourcesReply *reply,
               rpc::SendReplyCallback send_reply_callback),
              (override));
  MOCK_METHOD(void,
              HandleCancelResourceReserve,
              (rpc::CancelResourceReserveRequest request,
               rpc::CancelResourceReserveReply *reply,
               rpc::SendReplyCallback send_reply_callback),
              (override));
  MOCK_METHOD(void,
              HandleRequestWorkerLease,
              (rpc::RequestWorkerLeaseRequest request,
               rpc::RequestWorkerLeaseReply *reply,
               rpc::SendReplyCallback send_reply_callback),
              (override));
  MOCK_METHOD(void,
              HandleReportWorkerBacklog,
              (rpc::ReportWorkerBacklogRequest request,
               rpc::ReportWorkerBacklogReply *reply,
               rpc::SendReplyCallback send_reply_callback),
              (override));
  MOCK_METHOD(void,
              HandleReturnWorker,
              (rpc::ReturnWorkerRequest request,
               rpc::ReturnWorkerReply *reply,
               rpc::SendReplyCallback send_reply_callback),
              (override));
  MOCK_METHOD(void,
              HandleReleaseUnusedWorkers,
              (rpc::ReleaseUnusedWorkersRequest request,
               rpc::ReleaseUnusedWorkersReply *reply,
               rpc::SendReplyCallback send_reply_callback),
              (override));
  MOCK_METHOD(void,
              HandleCancelWorkerLease,
              (rpc::CancelWorkerLeaseRequest request,
               rpc::CancelWorkerLeaseReply *reply,
               rpc::SendReplyCallback send_reply_callback),
              (override));
  MOCK_METHOD(void,
              HandlePinObjectIDs,
              (rpc::PinObjectIDsRequest request,
               rpc::PinObjectIDsReply *reply,
               rpc::SendReplyCallback send_reply_callback),
              (override));
  MOCK_METHOD(void,
              HandleGetNodeStats,
              (rpc::GetNodeStatsRequest request,
               rpc::GetNodeStatsReply *reply,
               rpc::SendReplyCallback send_reply_callback),
              (override));
  MOCK_METHOD(void,
              HandleGlobalGC,
              (rpc::GlobalGCRequest request,
               rpc::GlobalGCReply *reply,
               rpc::SendReplyCallback send_reply_callback),
              (override));
  MOCK_METHOD(void,
              HandleFormatGlobalMemoryInfo,
              (rpc::FormatGlobalMemoryInfoRequest request,
               rpc::FormatGlobalMemoryInfoReply *reply,
               rpc::SendReplyCallback send_reply_callback),
              (override));
  MOCK_METHOD(void,
              HandleRequestObjectSpillage,
              (rpc::RequestObjectSpillageRequest request,
               rpc::RequestObjectSpillageReply *reply,
               rpc::SendReplyCallback send_reply_callback),
              (override));
  MOCK_METHOD(void,
              HandleReleaseUnusedBundles,
              (rpc::ReleaseUnusedBundlesRequest request,
               rpc::ReleaseUnusedBundlesReply *reply,
               rpc::SendReplyCallback send_reply_callback),
              (override));
  MOCK_METHOD(void,
              HandleGetSystemConfig,
              (rpc::GetSystemConfigRequest request,
               rpc::GetSystemConfigReply *reply,
               rpc::SendReplyCallback send_reply_callback),
              (override));
  MOCK_METHOD(void,
              HandleNotifyGCSRestart,
              (rpc::NotifyGCSRestartRequest request,
               rpc::NotifyGCSRestartReply *reply,
               rpc::SendReplyCallback send_reply_callback),
              (override));
  MOCK_METHOD(void,
              HandleGetGcsServerAddress,
              (rpc::GetGcsServerAddressRequest request,
               rpc::GetGcsServerAddressReply *reply,
               rpc::SendReplyCallback send_reply_callback),
              (override));
  MOCK_METHOD(void,
              HandleGetTaskFailureCause,
              (rpc::GetTaskFailureCauseRequest request,
               rpc::GetTaskFailureCauseReply *reply,
               rpc::SendReplyCallback send_reply_callback),
              (override));
};

}  // namespace raylet
}  // namespace ray
