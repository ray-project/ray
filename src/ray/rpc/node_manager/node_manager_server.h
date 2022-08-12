// Copyright 2017 The Ray Authors.
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

#include "ray/common/asio/instrumented_io_context.h"
#include "ray/rpc/grpc_server.h"
#include "ray/rpc/server_call.h"
#include "src/ray/protobuf/node_manager.grpc.pb.h"
#include "src/ray/protobuf/node_manager.pb.h"

namespace ray {
namespace rpc {

/// NOTE: See src/ray/core_worker/core_worker.h on how to add a new grpc handler.
#define RAY_NODE_MANAGER_RPC_HANDLERS                                 \
  RPC_SERVICE_HANDLER(NodeManagerService, UpdateResourceUsage, -1)    \
  RPC_SERVICE_HANDLER(NodeManagerService, RequestResourceReport, -1)  \
  RPC_SERVICE_HANDLER(NodeManagerService, GetResourceLoad, -1)        \
  RPC_SERVICE_HANDLER(NodeManagerService, NotifyGCSRestart, -1)       \
  RPC_SERVICE_HANDLER(NodeManagerService, RequestWorkerLease, -1)     \
  RPC_SERVICE_HANDLER(NodeManagerService, ReportWorkerBacklog, -1)    \
  RPC_SERVICE_HANDLER(NodeManagerService, ReturnWorker, -1)           \
  RPC_SERVICE_HANDLER(NodeManagerService, ReleaseUnusedWorkers, -1)   \
  RPC_SERVICE_HANDLER(NodeManagerService, CancelWorkerLease, -1)      \
  RPC_SERVICE_HANDLER(NodeManagerService, PinObjectIDs, -1)           \
  RPC_SERVICE_HANDLER(NodeManagerService, GetNodeStats, -1)           \
  RPC_SERVICE_HANDLER(NodeManagerService, GlobalGC, -1)               \
  RPC_SERVICE_HANDLER(NodeManagerService, FormatGlobalMemoryInfo, -1) \
  RPC_SERVICE_HANDLER(NodeManagerService, PrepareBundleResources, -1) \
  RPC_SERVICE_HANDLER(NodeManagerService, CommitBundleResources, -1)  \
  RPC_SERVICE_HANDLER(NodeManagerService, CancelResourceReserve, -1)  \
  RPC_SERVICE_HANDLER(NodeManagerService, RequestObjectSpillage, -1)  \
  RPC_SERVICE_HANDLER(NodeManagerService, ReleaseUnusedBundles, -1)   \
  RPC_SERVICE_HANDLER(NodeManagerService, GetSystemConfig, -1)        \
  RPC_SERVICE_HANDLER(NodeManagerService, ShutdownRaylet, -1)         \
  RPC_SERVICE_HANDLER(NodeManagerService, GetTasksInfo, -1)           \
  RPC_SERVICE_HANDLER(NodeManagerService, GetObjectsInfo, -1)

/// Interface of the `NodeManagerService`, see `src/ray/protobuf/node_manager.proto`.
class NodeManagerServiceHandler {
 public:
  /// Handlers. For all of the following handlers, the implementations can
  /// handle the request asynchronously. When handling is done, the
  /// `send_reply_callback` should be called. See
  /// src/ray/rpc/node_manager/node_manager_client.h and
  /// src/ray/protobuf/node_manager.proto for a description of the
  /// functionality of each handler.
  ///
  /// \param[in] request The request message.
  /// \param[out] reply The reply message.
  /// \param[in] send_reply_callback The callback to be called when the request is done.

  virtual void HandleUpdateResourceUsage(const rpc::UpdateResourceUsageRequest &request,
                                         rpc::UpdateResourceUsageReply *reply,
                                         rpc::SendReplyCallback send_reply_callback) = 0;

  virtual void HandleRequestResourceReport(
      const rpc::RequestResourceReportRequest &request,
      rpc::RequestResourceReportReply *reply,
      rpc::SendReplyCallback send_reply_callback) = 0;

  virtual void HandleGetResourceLoad(const rpc::GetResourceLoadRequest &request,
                                     rpc::GetResourceLoadReply *reply,
                                     rpc::SendReplyCallback send_reply_callback) = 0;

  virtual void HandleNotifyGCSRestart(const rpc::NotifyGCSRestartRequest &request,
                                      rpc::NotifyGCSRestartReply *reply,
                                      rpc::SendReplyCallback send_reply_callback) = 0;

  virtual void HandleRequestWorkerLease(const RequestWorkerLeaseRequest &request,
                                        RequestWorkerLeaseReply *reply,
                                        SendReplyCallback send_reply_callback) = 0;

  virtual void HandleReportWorkerBacklog(const ReportWorkerBacklogRequest &request,
                                         ReportWorkerBacklogReply *reply,
                                         SendReplyCallback send_reply_callback) = 0;

  virtual void HandleReturnWorker(const ReturnWorkerRequest &request,
                                  ReturnWorkerReply *reply,
                                  SendReplyCallback send_reply_callback) = 0;

  virtual void HandleReleaseUnusedWorkers(const ReleaseUnusedWorkersRequest &request,
                                          ReleaseUnusedWorkersReply *reply,
                                          SendReplyCallback send_reply_callback) = 0;

  virtual void HandleShutdownRaylet(const ShutdownRayletRequest &request,
                                    ShutdownRayletReply *reply,
                                    SendReplyCallback send_reply_callback) = 0;

  virtual void HandleCancelWorkerLease(const rpc::CancelWorkerLeaseRequest &request,
                                       rpc::CancelWorkerLeaseReply *reply,
                                       rpc::SendReplyCallback send_reply_callback) = 0;

  virtual void HandlePrepareBundleResources(
      const rpc::PrepareBundleResourcesRequest &request,
      rpc::PrepareBundleResourcesReply *reply,
      rpc::SendReplyCallback send_reply_callback) = 0;

  virtual void HandleCommitBundleResources(
      const rpc::CommitBundleResourcesRequest &request,
      rpc::CommitBundleResourcesReply *reply,
      rpc::SendReplyCallback send_reply_callback) = 0;

  virtual void HandleCancelResourceReserve(
      const rpc::CancelResourceReserveRequest &request,
      rpc::CancelResourceReserveReply *reply,
      rpc::SendReplyCallback send_reply_callback) = 0;

  virtual void HandlePinObjectIDs(const PinObjectIDsRequest &request,
                                  PinObjectIDsReply *reply,
                                  SendReplyCallback send_reply_callback) = 0;

  virtual void HandleGetNodeStats(const GetNodeStatsRequest &request,
                                  GetNodeStatsReply *reply,
                                  SendReplyCallback send_reply_callback) = 0;

  virtual void HandleGlobalGC(const GlobalGCRequest &request,
                              GlobalGCReply *reply,
                              SendReplyCallback send_reply_callback) = 0;

  virtual void HandleFormatGlobalMemoryInfo(const FormatGlobalMemoryInfoRequest &request,
                                            FormatGlobalMemoryInfoReply *reply,
                                            SendReplyCallback send_reply_callback) = 0;

  virtual void HandleRequestObjectSpillage(const RequestObjectSpillageRequest &request,
                                           RequestObjectSpillageReply *reply,
                                           SendReplyCallback send_reply_callback) = 0;

  virtual void HandleReleaseUnusedBundles(const ReleaseUnusedBundlesRequest &request,
                                          ReleaseUnusedBundlesReply *reply,
                                          SendReplyCallback send_reply_callback) = 0;

  virtual void HandleGetSystemConfig(const GetSystemConfigRequest &request,
                                     GetSystemConfigReply *reply,
                                     SendReplyCallback send_reply_callback) = 0;

  virtual void HandleGetTasksInfo(const GetTasksInfoRequest &request,
                                  GetTasksInfoReply *reply,
                                  SendReplyCallback send_reply_callback) = 0;

  virtual void HandleGetObjectsInfo(const GetObjectsInfoRequest &request,
                                    GetObjectsInfoReply *reply,
                                    SendReplyCallback send_reply_callback) = 0;
};

/// The `GrpcService` for `NodeManagerService`.
class NodeManagerGrpcService : public GrpcService {
 public:
  /// Constructor.
  ///
  /// \param[in] io_service See super class.
  /// \param[in] handler The service handler that actually handle the requests.
  NodeManagerGrpcService(instrumented_io_context &io_service,
                         NodeManagerServiceHandler &service_handler)
      : GrpcService(io_service), service_handler_(service_handler){};

 protected:
  grpc::Service &GetGrpcService() override { return service_; }

  void InitServerCallFactories(
      const std::unique_ptr<grpc::ServerCompletionQueue> &cq,
      std::vector<std::unique_ptr<ServerCallFactory>> *server_call_factories) override {
    RAY_NODE_MANAGER_RPC_HANDLERS
  }

 private:
  /// The grpc async service object.
  NodeManagerService::AsyncService service_;

  /// The service handler that actually handle the requests.
  NodeManagerServiceHandler &service_handler_;
};

}  // namespace rpc
}  // namespace ray
