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

#include <grpcpp/grpcpp.h>

#include <deque>
#include <limits>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <utility>

#include "absl/base/thread_annotations.h"
#include "absl/hash/hash.h"
#include "ray/common/status.h"
#include "ray/pubsub/subscriber.h"
#include "ray/rpc/retryable_grpc_client.h"
#include "ray/util/logging.h"
#include "src/ray/protobuf/core_worker.grpc.pb.h"
#include "src/ray/protobuf/core_worker.pb.h"

namespace std {
template <>
struct hash<ray::rpc::Address> {
  size_t operator()(const ray::rpc::Address &addr) const {
    size_t hash = std::hash<int32_t>()(addr.port());
    hash ^= std::hash<std::string>()(addr.ip_address());
    hash ^= std::hash<std::string>()(addr.worker_id());
    hash ^= std::hash<std::string>()(addr.raylet_id());
    return hash;
  }
};
}  // namespace std

namespace ray {
namespace rpc {

/// The maximum number of requests in flight per client.
inline constexpr int64_t kMaxBytesInFlight = 16L * 1024 * 1024;

/// The base size in bytes per request.
inline constexpr int64_t kBaseRequestSize = 1024;

/// Get the estimated size in bytes of the given task.
static const int64_t RequestSizeInBytes(const PushTaskRequest &request) {
  int64_t size = kBaseRequestSize;
  for (auto &arg : request.task_spec().args()) {
    size += arg.data().size();
  }
  return size;
}

// Shared between direct actor and task submitters.
/* class CoreWorkerClientInterface; */

inline bool operator==(const rpc::Address &lhs, const rpc::Address &rhs) {
  return google::protobuf::util::MessageDifferencer::Equivalent(lhs, rhs);
}

/// Abstract client interface for testing.
class CoreWorkerClientInterface : public pubsub::SubscriberClientInterface {
 public:
  virtual const rpc::Address &Addr() const {
    static const rpc::Address empty_addr_;
    return empty_addr_;
  }

  /// Returns true if the grpc channel is idle and there are no pending requests
  /// after at least one RPC call is made.
  virtual bool IsIdleAfterRPCs() const { return false; }

  /// Push an actor task directly from worker to worker.
  ///
  /// \param[in] request The request message.
  /// \param[in] skip_queue Whether to skip the task queue. This will send the
  /// task for execution immediately.
  /// \param[in] callback The callback function that handles reply.
  /// \return if the rpc call succeeds
  virtual void PushActorTask(std::unique_ptr<PushTaskRequest> request,
                             bool skip_queue,
                             ClientCallback<PushTaskReply> &&callback) {}

  /// Similar to PushActorTask, but sets no ordering constraint. This is used to
  /// push non-actor tasks directly to a worker.
  virtual void PushNormalTask(std::unique_ptr<PushTaskRequest> request,
                              const ClientCallback<PushTaskReply> &callback) {}

  /// Get the number of pending tasks for this worker.
  ///
  /// \param[in] request The request message.
  /// \param[in] callback The callback function that handles reply.
  /// \return if the rpc call succeeds
  virtual void NumPendingTasks(std::unique_ptr<NumPendingTasksRequest> request,
                               const ClientCallback<NumPendingTasksReply> &callback,
                               int64_t timeout_ms = -1) {}

  /// Notify a wait has completed for direct actor call arguments.
  ///
  /// \param[in] request The request message.
  /// \param[in] callback The callback function that handles reply.
  /// \return if the rpc call succeeds
  virtual void DirectActorCallArgWaitComplete(
      const DirectActorCallArgWaitCompleteRequest &request,
      const ClientCallback<DirectActorCallArgWaitCompleteReply> &callback) {}

  /// Ask the owner of an object about the object's current status.
  virtual void GetObjectStatus(const GetObjectStatusRequest &request,
                               const ClientCallback<GetObjectStatusReply> &callback) {}

  /// Ask the actor's owner to reply when the actor has no references.
  virtual void WaitForActorRefDeleted(
      const WaitForActorRefDeletedRequest &request,
      const ClientCallback<WaitForActorRefDeletedReply> &callback) {}

  /// Send a long polling request to a core worker for pubsub operations.
  virtual void PubsubLongPolling(const PubsubLongPollingRequest &request,
                                 const ClientCallback<PubsubLongPollingReply> &callback) {
  }

  /// Send a pubsub command batch request to a core worker for pubsub operations.
  virtual void PubsubCommandBatch(
      const PubsubCommandBatchRequest &request,
      const ClientCallback<PubsubCommandBatchReply> &callback) {}

  virtual void UpdateObjectLocationBatch(
      const UpdateObjectLocationBatchRequest &request,
      const ClientCallback<UpdateObjectLocationBatchReply> &callback) {}

  virtual void GetObjectLocationsOwner(
      const GetObjectLocationsOwnerRequest &request,
      const ClientCallback<GetObjectLocationsOwnerReply> &callback) {}

  virtual void ReportGeneratorItemReturns(
      const ReportGeneratorItemReturnsRequest &request,
      const ClientCallback<ReportGeneratorItemReturnsReply> &callback) {}

  /// Tell this actor to exit immediately.
  virtual void KillActor(const KillActorRequest &request,
                         const ClientCallback<KillActorReply> &callback) {}

  virtual void CancelTask(const CancelTaskRequest &request,
                          const ClientCallback<CancelTaskReply> &callback) {}

  virtual void RemoteCancelTask(const RemoteCancelTaskRequest &request,
                                const ClientCallback<RemoteCancelTaskReply> &callback) {}

  virtual void RegisterMutableObjectReader(
      const RegisterMutableObjectReaderRequest &request,
      const ClientCallback<RegisterMutableObjectReaderReply> &callback) {}

  virtual void GetCoreWorkerStats(
      const GetCoreWorkerStatsRequest &request,
      const ClientCallback<GetCoreWorkerStatsReply> &callback) {}

  virtual void LocalGC(const LocalGCRequest &request,
                       const ClientCallback<LocalGCReply> &callback) {}

  virtual void DeleteObjects(const DeleteObjectsRequest &request,
                             const ClientCallback<DeleteObjectsReply> &callback) {}

  virtual void SpillObjects(const SpillObjectsRequest &request,
                            const ClientCallback<SpillObjectsReply> &callback) {}

  virtual void RestoreSpilledObjects(
      const RestoreSpilledObjectsRequest &request,
      const ClientCallback<RestoreSpilledObjectsReply> &callback) {}

  virtual void DeleteSpilledObjects(
      const DeleteSpilledObjectsRequest &request,
      const ClientCallback<DeleteSpilledObjectsReply> &callback) {}

  virtual void PlasmaObjectReady(const PlasmaObjectReadyRequest &request,
                                 const ClientCallback<PlasmaObjectReadyReply> &callback) {
  }

  virtual void Exit(const ExitRequest &request,
                    const ClientCallback<ExitReply> &callback) {}

  virtual void AssignObjectOwner(const AssignObjectOwnerRequest &request,
                                 const ClientCallback<AssignObjectOwnerReply> &callback) {
  }

  virtual void RayletNotifyGCSRestart(
      const RayletNotifyGCSRestartRequest &request,
      const ClientCallback<RayletNotifyGCSRestartReply> &callback) {}

  /// Returns the max acked sequence number, useful for checking on progress.
  virtual int64_t ClientProcessedUpToSeqno() { return -1; }

  virtual ~CoreWorkerClientInterface() = default;
};

/// Client used for communicating with a remote worker server.
class CoreWorkerClient : public std::enable_shared_from_this<CoreWorkerClient>,
                         public CoreWorkerClientInterface {
 public:
  /// Constructor.
  ///
  /// \param[in] address Address of the worker server.
  /// \param[in] client_call_manager The `ClientCallManager` used for managing requests.
  CoreWorkerClient(const rpc::Address &address,
                   ClientCallManager &client_call_manager,
                   std::function<void()> core_worker_unavailable_timeout_callback)
      : addr_(address) {
    grpc_client_ = std::make_shared<GrpcClient<CoreWorkerService>>(
        addr_.ip_address(), addr_.port(), client_call_manager);

    retryable_grpc_client_ = RetryableGrpcClient::Create(
        grpc_client_->Channel(),
        client_call_manager.GetMainService(),
        /*max_pending_requests_bytes=*/
        std::numeric_limits<uint64_t>::max(),
        /*check_channel_status_interval_milliseconds=*/
        ::RayConfig::instance()
            .grpc_client_check_connection_status_interval_milliseconds(),
        /*server_unavailable_timeout_seconds=*/
        ::RayConfig::instance().core_worker_rpc_server_reconnect_timeout_s(),
        /*server_unavailable_timeout_callback=*/
        core_worker_unavailable_timeout_callback,
        /*server_name=*/"Core worker " + addr_.ip_address());
  };

  const rpc::Address &Addr() const override { return addr_; }

  bool IsIdleAfterRPCs() const override {
    return grpc_client_->IsChannelIdleAfterRPCs() &&
           (retryable_grpc_client_->NumPendingRequests() == 0);
  }

  VOID_RPC_CLIENT_METHOD(CoreWorkerService,
                         DirectActorCallArgWaitComplete,
                         grpc_client_,
                         /*method_timeout_ms*/ -1,
                         override)

  VOID_RPC_CLIENT_METHOD(CoreWorkerService,
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

  VOID_RPC_CLIENT_METHOD(CoreWorkerService,
                         PubsubLongPolling,
                         grpc_client_,
                         /*method_timeout_ms*/ -1,
                         override)

  VOID_RPC_CLIENT_METHOD(CoreWorkerService,
                         PubsubCommandBatch,
                         grpc_client_,
                         /*method_timeout_ms*/ -1,
                         override)

  VOID_RPC_CLIENT_METHOD(CoreWorkerService,
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
                     ClientCallback<PushTaskReply> &&callback) override {
    if (skip_queue) {
      // Set this value so that the actor does not skip any tasks when
      // processing this request. We could also set it to max_finished_seq_no_,
      // but we just set it to the default of -1 to avoid taking the lock.
      request->set_client_processed_up_to(-1);
      INVOKE_RPC_CALL(CoreWorkerService,
                      PushTask,
                      *request,
                      callback,
                      grpc_client_,
                      /*method_timeout_ms*/ -1);
      return;
    }

    {
      absl::MutexLock lock(&mutex_);
      send_queue_.emplace_back(std::move(request), std::move(callback));
    }
    SendRequests();
  }

  void PushNormalTask(std::unique_ptr<PushTaskRequest> request,
                      const ClientCallback<PushTaskReply> &callback) override {
    request->set_sequence_number(-1);
    request->set_client_processed_up_to(-1);
    INVOKE_RPC_CALL(CoreWorkerService,
                    PushTask,
                    *request,
                    callback,
                    grpc_client_,
                    /*method_timeout_ms*/ -1);
  }

  void NumPendingTasks(std::unique_ptr<NumPendingTasksRequest> request,
                       const ClientCallback<NumPendingTasksReply> &callback,
                       int64_t timeout_ms = -1) override {
    INVOKE_RPC_CALL(
        CoreWorkerService, NumPendingTasks, *request, callback, grpc_client_, timeout_ms);
  }

  /// Send as many pending tasks as possible. This method is thread-safe.
  ///
  /// The client will guarantee no more than kMaxBytesInFlight bytes of RPCs are being
  /// sent at once. This prevents the server scheduling queue from being overwhelmed.
  /// See direct_actor.proto for a description of the ordering protocol.
  void SendRequests() {
    absl::MutexLock lock(&mutex_);
    auto this_ptr = this->shared_from_this();

    while (!send_queue_.empty() && rpc_bytes_in_flight_ < kMaxBytesInFlight) {
      auto pair = std::move(*send_queue_.begin());
      send_queue_.pop_front();

      auto request = std::move(pair.first);
      int64_t task_size = RequestSizeInBytes(*request);
      int64_t seq_no = request->sequence_number();
      request->set_client_processed_up_to(max_finished_seq_no_);
      rpc_bytes_in_flight_ += task_size;

      auto rpc_callback =
          [this, this_ptr, seq_no, task_size, callback = std::move(pair.second)](
              Status status, rpc::PushTaskReply &&reply) {
            {
              absl::MutexLock lock(&mutex_);
              if (seq_no > max_finished_seq_no_) {
                max_finished_seq_no_ = seq_no;
              }
              rpc_bytes_in_flight_ -= task_size;
              RAY_CHECK(rpc_bytes_in_flight_ >= 0);
            }
            SendRequests();
            callback(status, std::move(reply));
          };

      RAY_UNUSED(INVOKE_RPC_CALL(CoreWorkerService,
                                 PushTask,
                                 *request,
                                 std::move(rpc_callback),
                                 grpc_client_,
                                 /*method_timeout_ms*/ -1));
    }

    if (!send_queue_.empty()) {
      RAY_LOG(DEBUG) << "client send queue size " << send_queue_.size();
    }
  }

  /// Returns the max acked sequence number, useful for checking on progress.
  int64_t ClientProcessedUpToSeqno() override {
    absl::MutexLock lock(&mutex_);
    return max_finished_seq_no_;
  }

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
  int64_t max_finished_seq_no_ ABSL_GUARDED_BY(mutex_) = -1;
};

using CoreWorkerClientFactoryFn =
    std::function<std::shared_ptr<CoreWorkerClientInterface>(const rpc::Address &)>;

}  // namespace rpc
}  // namespace ray
