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

/*
 * This file defines the gRPC service *INTERFACES* only.
 * The subcomponent that handles a given interface should inherit from the relevant
 * class. The target for the subcomponent should depend only on this file, not on
 * grpc_services.h.
 */

#pragma once

#include "ray/common/status.h"
#include "src/ray/protobuf/gcs_service.grpc.pb.h"

namespace ray {
namespace rpc {

using SendReplyCallback = std::function<void(
    Status status, std::function<void()> success, std::function<void()> failure)>;

#define GCS_RPC_SEND_REPLY(send_reply_callback, reply, status)        \
  reply->mutable_status()->set_code(static_cast<int>(status.code())); \
  reply->mutable_status()->set_message(status.message());             \
  send_reply_callback(ray::Status::OK(), nullptr, nullptr)

class NodeInfoGcsServiceHandler {
 public:
  virtual ~NodeInfoGcsServiceHandler() = default;

  virtual void HandleGetClusterId(GetClusterIdRequest request,
                                  GetClusterIdReply *reply,
                                  SendReplyCallback send_reply_callback) = 0;

  virtual void HandleRegisterNode(RegisterNodeRequest request,
                                  RegisterNodeReply *reply,
                                  SendReplyCallback send_reply_callback) = 0;

  virtual void HandleUnregisterNode(UnregisterNodeRequest request,
                                    UnregisterNodeReply *reply,
                                    SendReplyCallback send_reply_callback) = 0;

  virtual void HandleCheckAlive(CheckAliveRequest request,
                                CheckAliveReply *reply,
                                SendReplyCallback send_reply_callback) = 0;

  virtual void HandleDrainNode(DrainNodeRequest request,
                               DrainNodeReply *reply,
                               SendReplyCallback send_reply_callback) = 0;

  virtual void HandleGetAllNodeInfo(GetAllNodeInfoRequest request,
                                    GetAllNodeInfoReply *reply,
                                    SendReplyCallback send_reply_callback) = 0;
};

class NodeResourceInfoGcsServiceHandler {
 public:
  virtual ~NodeResourceInfoGcsServiceHandler() = default;

  virtual void HandleGetAllAvailableResources(GetAllAvailableResourcesRequest request,
                                              GetAllAvailableResourcesReply *reply,
                                              SendReplyCallback send_reply_callback) = 0;

  virtual void HandleGetAllTotalResources(GetAllTotalResourcesRequest request,
                                          GetAllTotalResourcesReply *reply,
                                          SendReplyCallback send_reply_callback) = 0;

  virtual void HandleGetDrainingNodes(GetDrainingNodesRequest request,
                                      GetDrainingNodesReply *reply,
                                      SendReplyCallback send_reply_callback) = 0;

  virtual void HandleGetAllResourceUsage(GetAllResourceUsageRequest request,
                                         GetAllResourceUsageReply *reply,
                                         SendReplyCallback send_reply_callback) = 0;
};

class InternalPubSubGcsServiceHandler {
 public:
  virtual ~InternalPubSubGcsServiceHandler() = default;

  virtual void HandleGcsPublish(GcsPublishRequest request,
                                GcsPublishReply *reply,
                                SendReplyCallback send_reply_callback) = 0;

  virtual void HandleGcsSubscriberPoll(GcsSubscriberPollRequest request,
                                       GcsSubscriberPollReply *reply,
                                       SendReplyCallback send_reply_callback) = 0;

  virtual void HandleGcsSubscriberCommandBatch(GcsSubscriberCommandBatchRequest request,
                                               GcsSubscriberCommandBatchReply *reply,
                                               SendReplyCallback send_reply_callback) = 0;

  virtual void HandleGcsUnregisterSubscriber(GcsUnregisterSubscriberRequest request,
                                             GcsUnregisterSubscriberReply *reply,
                                             SendReplyCallback send_reply_callback) = 0;
};

class JobInfoGcsServiceHandler {
 public:
  using JobFinishListenerCallback = std::function<void(const rpc::JobTableData &)>;

  virtual ~JobInfoGcsServiceHandler() = default;

  virtual void HandleAddJob(AddJobRequest request,
                            AddJobReply *reply,
                            SendReplyCallback send_reply_callback) = 0;

  virtual void HandleMarkJobFinished(MarkJobFinishedRequest request,
                                     MarkJobFinishedReply *reply,
                                     SendReplyCallback send_reply_callback) = 0;

  virtual void HandleGetAllJobInfo(GetAllJobInfoRequest request,
                                   GetAllJobInfoReply *reply,
                                   SendReplyCallback send_reply_callback) = 0;

  virtual void AddJobFinishedListener(JobFinishListenerCallback listener) = 0;

  virtual void HandleReportJobError(ReportJobErrorRequest request,
                                    ReportJobErrorReply *reply,
                                    SendReplyCallback send_reply_callback) = 0;

  virtual void HandleGetNextJobID(GetNextJobIDRequest request,
                                  GetNextJobIDReply *reply,
                                  SendReplyCallback send_reply_callback) = 0;
};

class RuntimeEnvGcsServiceHandler {
 public:
  virtual ~RuntimeEnvGcsServiceHandler() = default;

  virtual void HandlePinRuntimeEnvURI(PinRuntimeEnvURIRequest request,
                                      PinRuntimeEnvURIReply *reply,
                                      SendReplyCallback send_reply_callback) = 0;
};

class WorkerInfoGcsServiceHandler {
 public:
  virtual ~WorkerInfoGcsServiceHandler() = default;

  virtual void HandleReportWorkerFailure(ReportWorkerFailureRequest request,
                                         ReportWorkerFailureReply *reply,
                                         SendReplyCallback send_reply_callback) = 0;

  virtual void HandleGetWorkerInfo(GetWorkerInfoRequest request,
                                   GetWorkerInfoReply *reply,
                                   SendReplyCallback send_reply_callback) = 0;

  virtual void HandleGetAllWorkerInfo(GetAllWorkerInfoRequest request,
                                      GetAllWorkerInfoReply *reply,
                                      SendReplyCallback send_reply_callback) = 0;

  virtual void HandleAddWorkerInfo(AddWorkerInfoRequest request,
                                   AddWorkerInfoReply *reply,
                                   SendReplyCallback send_reply_callback) = 0;

  virtual void HandleUpdateWorkerDebuggerPort(UpdateWorkerDebuggerPortRequest request,
                                              UpdateWorkerDebuggerPortReply *reply,
                                              SendReplyCallback send_reply_callback) = 0;

  virtual void HandleUpdateWorkerNumPausedThreads(
      UpdateWorkerNumPausedThreadsRequest request,
      UpdateWorkerNumPausedThreadsReply *reply,
      SendReplyCallback send_reply_callback) = 0;
};

}  // namespace rpc
}  // namespace ray
