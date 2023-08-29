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

/// TODO(vitsai): Remove this when auth is implemented for node manager
#define RAY_NODE_MANAGER_RPC_SERVICE_HANDLER(METHOD) \
  RPC_SERVICE_HANDLER_CUSTOM_AUTH(NodeManagerService, METHOD, -1, AuthType::NO_AUTH)

/// NOTE: See src/ray/core_worker/core_worker.h on how to add a new grpc handler.
#define RAY_NODE_MANAGER_RPC_HANDLERS                          \
  RAY_NODE_MANAGER_RPC_SERVICE_HANDLER(GetResourceLoad)        \
  RAY_NODE_MANAGER_RPC_SERVICE_HANDLER(NotifyGCSRestart)       \
  RAY_NODE_MANAGER_RPC_SERVICE_HANDLER(RequestWorkerLease)     \
  RAY_NODE_MANAGER_RPC_SERVICE_HANDLER(ReportWorkerBacklog)    \
  RAY_NODE_MANAGER_RPC_SERVICE_HANDLER(ReturnWorker)           \
  RAY_NODE_MANAGER_RPC_SERVICE_HANDLER(ReleaseUnusedWorkers)   \
  RAY_NODE_MANAGER_RPC_SERVICE_HANDLER(CancelWorkerLease)      \
  RAY_NODE_MANAGER_RPC_SERVICE_HANDLER(PinObjectIDs)           \
  RAY_NODE_MANAGER_RPC_SERVICE_HANDLER(GetNodeStats)           \
  RAY_NODE_MANAGER_RPC_SERVICE_HANDLER(GlobalGC)               \
  RAY_NODE_MANAGER_RPC_SERVICE_HANDLER(FormatGlobalMemoryInfo) \
  RAY_NODE_MANAGER_RPC_SERVICE_HANDLER(PrepareBundleResources) \
  RAY_NODE_MANAGER_RPC_SERVICE_HANDLER(CommitBundleResources)  \
  RAY_NODE_MANAGER_RPC_SERVICE_HANDLER(CancelResourceReserve)  \
  RAY_NODE_MANAGER_RPC_SERVICE_HANDLER(RequestObjectSpillage)  \
  RAY_NODE_MANAGER_RPC_SERVICE_HANDLER(ReleaseUnusedBundles)   \
  RAY_NODE_MANAGER_RPC_SERVICE_HANDLER(GetSystemConfig)        \
  RAY_NODE_MANAGER_RPC_SERVICE_HANDLER(ShutdownRaylet)         \
  RAY_NODE_MANAGER_RPC_SERVICE_HANDLER(DrainRaylet)            \
  RAY_NODE_MANAGER_RPC_SERVICE_HANDLER(GetTasksInfo)           \
  RAY_NODE_MANAGER_RPC_SERVICE_HANDLER(GetObjectsInfo)         \
  RAY_NODE_MANAGER_RPC_SERVICE_HANDLER(GetTaskFailureCause)

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

  virtual void HandleGetResourceLoad(rpc::GetResourceLoadRequest request,
                                     rpc::GetResourceLoadReply *reply,
                                     rpc::SendReplyCallback send_reply_callback) = 0;

  virtual void HandleNotifyGCSRestart(rpc::NotifyGCSRestartRequest request,
                                      rpc::NotifyGCSRestartReply *reply,
                                      rpc::SendReplyCallback send_reply_callback) = 0;

  virtual void HandleRequestWorkerLease(RequestWorkerLeaseRequest request,
                                        RequestWorkerLeaseReply *reply,
                                        SendReplyCallback send_reply_callback) = 0;

  virtual void HandleReportWorkerBacklog(ReportWorkerBacklogRequest request,
                                         ReportWorkerBacklogReply *reply,
                                         SendReplyCallback send_reply_callback) = 0;

  virtual void HandleReturnWorker(ReturnWorkerRequest request,
                                  ReturnWorkerReply *reply,
                                  SendReplyCallback send_reply_callback) = 0;

  virtual void HandleReleaseUnusedWorkers(ReleaseUnusedWorkersRequest request,
                                          ReleaseUnusedWorkersReply *reply,
                                          SendReplyCallback send_reply_callback) = 0;

  virtual void HandleShutdownRaylet(ShutdownRayletRequest request,
                                    ShutdownRayletReply *reply,
                                    SendReplyCallback send_reply_callback) = 0;

  virtual void HandleDrainRaylet(rpc::DrainRayletRequest request,
                                 rpc::DrainRayletReply *reply,
                                 SendReplyCallback send_reply_callback) = 0;

  virtual void HandleCancelWorkerLease(rpc::CancelWorkerLeaseRequest request,
                                       rpc::CancelWorkerLeaseReply *reply,
                                       rpc::SendReplyCallback send_reply_callback) = 0;

  virtual void HandlePrepareBundleResources(
      rpc::PrepareBundleResourcesRequest request,
      rpc::PrepareBundleResourcesReply *reply,
      rpc::SendReplyCallback send_reply_callback) = 0;

  virtual void HandleCommitBundleResources(
      rpc::CommitBundleResourcesRequest request,
      rpc::CommitBundleResourcesReply *reply,
      rpc::SendReplyCallback send_reply_callback) = 0;

  virtual void HandleCancelResourceReserve(
      rpc::CancelResourceReserveRequest request,
      rpc::CancelResourceReserveReply *reply,
      rpc::SendReplyCallback send_reply_callback) = 0;

  virtual void HandlePinObjectIDs(PinObjectIDsRequest request,
                                  PinObjectIDsReply *reply,
                                  SendReplyCallback send_reply_callback) = 0;

  virtual void HandleGetNodeStats(GetNodeStatsRequest request,
                                  GetNodeStatsReply *reply,
                                  SendReplyCallback send_reply_callback) = 0;

  virtual void HandleGlobalGC(GlobalGCRequest request,
                              GlobalGCReply *reply,
                              SendReplyCallback send_reply_callback) = 0;

  virtual void HandleFormatGlobalMemoryInfo(FormatGlobalMemoryInfoRequest request,
                                            FormatGlobalMemoryInfoReply *reply,
                                            SendReplyCallback send_reply_callback) = 0;

  virtual void HandleRequestObjectSpillage(RequestObjectSpillageRequest request,
                                           RequestObjectSpillageReply *reply,
                                           SendReplyCallback send_reply_callback) = 0;

  virtual void HandleReleaseUnusedBundles(ReleaseUnusedBundlesRequest request,
                                          ReleaseUnusedBundlesReply *reply,
                                          SendReplyCallback send_reply_callback) = 0;

  virtual void HandleGetSystemConfig(GetSystemConfigRequest request,
                                     GetSystemConfigReply *reply,
                                     SendReplyCallback send_reply_callback) = 0;

  virtual void HandleGetTasksInfo(GetTasksInfoRequest request,
                                  GetTasksInfoReply *reply,
                                  SendReplyCallback send_reply_callback) = 0;

  virtual void HandleGetObjectsInfo(GetObjectsInfoRequest request,
                                    GetObjectsInfoReply *reply,
                                    SendReplyCallback send_reply_callback) = 0;

  virtual void HandleGetTaskFailureCause(GetTaskFailureCauseRequest request,
                                         GetTaskFailureCauseReply *reply,
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
      std::vector<std::unique_ptr<ServerCallFactory>> *server_call_factories,
      const ClusterID &cluster_id) override {
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
