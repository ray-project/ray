namespace ray {
namespace raylet {

class MockNodeManagerConfig : public NodeManagerConfig {
 public:
};

}  // namespace raylet
}  // namespace ray

namespace ray {
namespace raylet {

class MockHeartbeatSender : public HeartbeatSender {
 public:
};

}  // namespace raylet
}  // namespace ray

namespace ray {
namespace raylet {

class MockNodeManager : public NodeManager {
 public:
  MOCK_METHOD(void, HandleUpdateResourceUsage, (const rpc::UpdateResourceUsageRequest &request, rpc::UpdateResourceUsageReply *reply, rpc::SendReplyCallback send_reply_callback), (override));
  MOCK_METHOD(void, HandleRequestResourceReport, (const rpc::RequestResourceReportRequest &request, rpc::RequestResourceReportReply *reply, rpc::SendReplyCallback send_reply_callback), (override));
  MOCK_METHOD(void, HandlePrepareBundleResources, (const rpc::PrepareBundleResourcesRequest &request, rpc::PrepareBundleResourcesReply *reply, rpc::SendReplyCallback send_reply_callback), (override));
  MOCK_METHOD(void, HandleCommitBundleResources, (const rpc::CommitBundleResourcesRequest &request, rpc::CommitBundleResourcesReply *reply, rpc::SendReplyCallback send_reply_callback), (override));
  MOCK_METHOD(void, HandleCancelResourceReserve, (const rpc::CancelResourceReserveRequest &request, rpc::CancelResourceReserveReply *reply, rpc::SendReplyCallback send_reply_callback), (override));
  MOCK_METHOD(void, HandleRequestWorkerLease, (const rpc::RequestWorkerLeaseRequest &request, rpc::RequestWorkerLeaseReply *reply, rpc::SendReplyCallback send_reply_callback), (override));
  MOCK_METHOD(void, HandleReturnWorker, (const rpc::ReturnWorkerRequest &request, rpc::ReturnWorkerReply *reply, rpc::SendReplyCallback send_reply_callback), (override));
  MOCK_METHOD(void, HandleReleaseUnusedWorkers, (const rpc::ReleaseUnusedWorkersRequest &request, rpc::ReleaseUnusedWorkersReply *reply, rpc::SendReplyCallback send_reply_callback), (override));
  MOCK_METHOD(void, HandleCancelWorkerLease, (const rpc::CancelWorkerLeaseRequest &request, rpc::CancelWorkerLeaseReply *reply, rpc::SendReplyCallback send_reply_callback), (override));
  MOCK_METHOD(void, HandlePinObjectIDs, (const rpc::PinObjectIDsRequest &request, rpc::PinObjectIDsReply *reply, rpc::SendReplyCallback send_reply_callback), (override));
  MOCK_METHOD(void, HandleGetNodeStats, (const rpc::GetNodeStatsRequest &request, rpc::GetNodeStatsReply *reply, rpc::SendReplyCallback send_reply_callback), (override));
  MOCK_METHOD(void, HandleGlobalGC, (const rpc::GlobalGCRequest &request, rpc::GlobalGCReply *reply, rpc::SendReplyCallback send_reply_callback), (override));
  MOCK_METHOD(void, HandleFormatGlobalMemoryInfo, (const rpc::FormatGlobalMemoryInfoRequest &request, rpc::FormatGlobalMemoryInfoReply *reply, rpc::SendReplyCallback send_reply_callback), (override));
  MOCK_METHOD(void, HandleRequestObjectSpillage, (const rpc::RequestObjectSpillageRequest &request, rpc::RequestObjectSpillageReply *reply, rpc::SendReplyCallback send_reply_callback), (override));
  MOCK_METHOD(void, HandleReleaseUnusedBundles, (const rpc::ReleaseUnusedBundlesRequest &request, rpc::ReleaseUnusedBundlesReply *reply, rpc::SendReplyCallback send_reply_callback), (override));
  MOCK_METHOD(void, HandleGetSystemConfig, (const rpc::GetSystemConfigRequest &request, rpc::GetSystemConfigReply *reply, rpc::SendReplyCallback send_reply_callback), (override));
  MOCK_METHOD(void, HandleGetGcsServerAddress, (const rpc::GetGcsServerAddressRequest &request, rpc::GetGcsServerAddressReply *reply, rpc::SendReplyCallback send_reply_callback), (override));
};

}  // namespace raylet
}  // namespace ray
