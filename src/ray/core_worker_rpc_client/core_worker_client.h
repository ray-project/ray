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

#include <deque>
#include <memory>
#include <optional>
#include <string>
#include <utility>

#include "absl/base/thread_annotations.h"
#include "absl/synchronization/mutex.h"
#include "ray/core_worker_rpc_client/core_worker_client_interface.h"
#include "ray/rpc/retryable_grpc_client.h"
#include "ray/rpc/rpc_callback_types.h"
#include "src/ray/protobuf/core_worker.grpc.pb.h"
#include "src/ray/protobuf/core_worker.pb.h"

namespace ray {
namespace rpc {

/// The maximum number of requests in flight per client.
inline constexpr int64_t kMaxBytesInFlight = 16L * 1024 * 1024;

/// The base size in bytes per request.
inline constexpr int64_t kBaseRequestSize = 1024;

/// Client used for communicating with a remote worker server.
class CoreWorkerClient : public std::enable_shared_from_this<CoreWorkerClient>,
                         public CoreWorkerClientInterface {
 public:
  /// Constructor.
  ///
  /// \param[in] address Address of the worker server.
  /// \param[in] client_call_manager The `ClientCallManager` used for managing requests.
  /// \param[in] core_worker_unavailable_timeout_callback The callback function that is
  /// used by the retryable grpc to remove unresponsive core worker connections from the
  /// pool once its been unavailable for more than server_unavailable_timeout_seconds.
  CoreWorkerClient(rpc::Address address,
                   ClientCallManager &client_call_manager,
                   std::function<void()> core_worker_unavailable_timeout_callback);

  const rpc::Address &Addr() const override { return addr_; }

  bool IsIdleAfterRPCs() const override {
    return grpc_client_->IsChannelIdleAfterRPCs() &&
           retryable_grpc_client_->NumActiveRequests() == 0;
  }

  VOID_RPC_CLIENT_METHOD(CoreWorkerService,
                         ActorCallArgWaitComplete,
                         grpc_client_,
                         /*method_timeout_ms*/ -1,
                         override)

  VOID_RETRYABLE_RPC_CLIENT_METHOD(retryable_grpc_client_,
                                   CoreWorkerService,
                                   GetObjectStatus,
                                   grpc_client_,
                                   /*method_timeout_ms*/ -1,
                                   override)

  VOID_RPC_CLIENT_METHOD(CoreWorkerService,
                         KillActor,
                         grpc_client_,
                         /*method_timeout_ms*/ -1,
                         override)

  VOID_RPC_CLIENT_METHOD(CoreWorkerService,
                         CancelTask,
                         grpc_client_,
                         /*method_timeout_ms*/ -1,
                         override)

  VOID_RPC_CLIENT_METHOD(CoreWorkerService,
                         RemoteCancelTask,
                         grpc_client_,
                         /*method_timeout_ms*/ -1,
                         override)

  VOID_RPC_CLIENT_METHOD(CoreWorkerService,
                         WaitForActorRefDeleted,
                         grpc_client_,
                         /*method_timeout_ms*/ -1,
                         override)

  VOID_RETRYABLE_RPC_CLIENT_METHOD(retryable_grpc_client_,
                                   CoreWorkerService,
                                   PubsubLongPolling,
                                   grpc_client_,
                                   /*method_timeout_ms*/ -1,
                                   override)

  VOID_RETRYABLE_RPC_CLIENT_METHOD(retryable_grpc_client_,
                                   CoreWorkerService,
                                   PubsubCommandBatch,
                                   grpc_client_,
                                   /*method_timeout_ms*/ -1,
                                   override)

  VOID_RETRYABLE_RPC_CLIENT_METHOD(retryable_grpc_client_,
                                   CoreWorkerService,
                                   UpdateObjectLocationBatch,
                                   grpc_client_,
                                   /*method_timeout_ms*/ -1,
                                   override)

  VOID_RPC_CLIENT_METHOD(CoreWorkerService,
                         GetObjectLocationsOwner,
                         grpc_client_,
                         /*method_timeout_ms*/ -1,
                         override)

  VOID_RETRYABLE_RPC_CLIENT_METHOD(retryable_grpc_client_,
                                   CoreWorkerService,
                                   ReportGeneratorItemReturns,
                                   grpc_client_,
                                   /*method_timeout_ms*/ -1,
                                   override)

  VOID_RPC_CLIENT_METHOD(CoreWorkerService,
                         RegisterMutableObjectReader,
                         grpc_client_,
                         /*method_timeout_ms*/ -1,
                         override)

  VOID_RPC_CLIENT_METHOD(CoreWorkerService,
                         GetCoreWorkerStats,
                         grpc_client_,
                         /*method_timeout_ms*/ -1,
                         override)

  VOID_RPC_CLIENT_METHOD(CoreWorkerService,
                         LocalGC,
                         grpc_client_,
                         /*method_timeout_ms*/ -1,
                         override)

  VOID_RPC_CLIENT_METHOD(CoreWorkerService,
                         DeleteObjects,
                         grpc_client_,
                         /*method_timeout_ms*/ -1,
                         override)

  VOID_RPC_CLIENT_METHOD(CoreWorkerService,
                         SpillObjects,
                         grpc_client_,
                         /*method_timeout_ms*/ -1,
                         override)

  VOID_RPC_CLIENT_METHOD(CoreWorkerService,
                         RestoreSpilledObjects,
                         grpc_client_,
                         /*method_timeout_ms*/ -1,
                         override)

  VOID_RPC_CLIENT_METHOD(CoreWorkerService,
                         DeleteSpilledObjects,
                         grpc_client_,
                         /*method_timeout_ms*/ -1,
                         override)

  VOID_RPC_CLIENT_METHOD(CoreWorkerService,
                         PlasmaObjectReady,
                         grpc_client_,
                         /*method_timeout_ms*/ -1,
                         override)

  VOID_RPC_CLIENT_METHOD(CoreWorkerService,
                         RayletNotifyGCSRestart,
                         grpc_client_,
                         /*method_timeout_ms*/ -1,
                         override)

  VOID_RPC_CLIENT_METHOD(
      CoreWorkerService, Exit, grpc_client_, /*method_timeout_ms*/ -1, override)

  VOID_RPC_CLIENT_METHOD(CoreWorkerService,
                         AssignObjectOwner,
                         grpc_client_,
                         /*method_timeout_ms*/ -1,
                         override)

  void PushActorTask(std::unique_ptr<PushTaskRequest> request,
                     bool skip_queue,
                     ClientCallback<PushTaskReply> &&callback) override;

  void PushNormalTask(std::unique_ptr<PushTaskRequest> request,
                      const ClientCallback<PushTaskReply> &callback) override;

  void NumPendingTasks(std::unique_ptr<NumPendingTasksRequest> request,
                       const ClientCallback<NumPendingTasksReply> &callback,
                       int64_t timeout_ms = -1) override {
    INVOKE_RPC_CALL(
        CoreWorkerService, NumPendingTasks, *request, callback, grpc_client_, timeout_ms);
  }

  std::string DebugString() const override { return ""; }

  /// Send as many pending tasks as possible. This method is thread-safe.
  ///
  /// The client will guarantee no more than kMaxBytesInFlight bytes of RPCs are being
  /// sent at once. This prevents the server scheduling queue from being overwhelmed.
  /// See direct_actor.proto for a description of the ordering protocol.
  void SendRequests();

 private:
  /// Protects against unsafe concurrent access from the callback thread.
  absl::Mutex mutex_;

  /// Address of the remote worker.
  rpc::Address addr_;

  /// The RPC client.
  std::shared_ptr<GrpcClient<CoreWorkerService>> grpc_client_;

  std::shared_ptr<RetryableGrpcClient> retryable_grpc_client_;

  /// Queue of requests to send.
  std::deque<std::pair<std::unique_ptr<PushTaskRequest>, ClientCallback<PushTaskReply>>>
      send_queue_ ABSL_GUARDED_BY(mutex_);

  /// The number of bytes currently in flight.
  int64_t rpc_bytes_in_flight_ ABSL_GUARDED_BY(mutex_) = 0;

  /// The max sequence number we have processed responses for.
  std::optional<int64_t> max_finished_seq_no_ ABSL_GUARDED_BY(mutex_);
};

}  // namespace rpc
}  // namespace ray
