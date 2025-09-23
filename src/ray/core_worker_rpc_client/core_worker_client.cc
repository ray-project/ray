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

#include "ray/core_worker_rpc_client/core_worker_client.h"

#include <limits>
#include <memory>
#include <utility>

#include "ray/util/logging.h"

namespace ray {
namespace rpc {

CoreWorkerClient::CoreWorkerClient(
    rpc::Address address,
    ClientCallManager &client_call_manager,
    std::function<void()> core_worker_unavailable_timeout_callback)
    : addr_(std::move(address)),
      grpc_client_(std::make_shared<GrpcClient<CoreWorkerService>>(
          addr_.ip_address(), addr_.port(), client_call_manager)),
      retryable_grpc_client_(RetryableGrpcClient::Create(
          grpc_client_->Channel(),
          client_call_manager.GetMainService(),
          /*max_pending_requests_bytes=*/std::numeric_limits<uint64_t>::max(),
          /*check_channel_status_interval_milliseconds=*/
          ::RayConfig::instance()
              .grpc_client_check_connection_status_interval_milliseconds(),
          /*server_unavailable_timeout_seconds=*/
          ::RayConfig::instance().core_worker_rpc_server_reconnect_timeout_s(),
          /*server_unavailable_timeout_callback=*/
          std::move(core_worker_unavailable_timeout_callback),
          /*server_name=*/"Core worker " + addr_.ip_address())) {}

void CoreWorkerClient::PushActorTask(std::unique_ptr<PushTaskRequest> request,
                                     bool skip_queue,
                                     ClientCallback<PushTaskReply> &&callback) {
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
    if (max_finished_seq_no_ == std::nullopt) {
      max_finished_seq_no_ = request->sequence_number() - 1;
    }
    // The RPC client assumes that the first request put into the send queue will be
    // the first task handled by the server.
    RAY_CHECK_LE(max_finished_seq_no_.value(), request->sequence_number());
    send_queue_.emplace_back(std::move(request), std::move(callback));
  }
  SendRequests();
}

void CoreWorkerClient::PushNormalTask(std::unique_ptr<PushTaskRequest> request,
                                      const ClientCallback<PushTaskReply> &callback) {
  request->set_sequence_number(-1);
  request->set_client_processed_up_to(-1);
  INVOKE_RPC_CALL(CoreWorkerService,
                  PushTask,
                  *request,
                  callback,
                  grpc_client_,
                  /*method_timeout_ms*/ -1);
}

/// Get the estimated size in bytes of the given task.
static const int64_t RequestSizeInBytes(const PushTaskRequest &request) {
  int64_t size = kBaseRequestSize;
  for (auto &arg : request.task_spec().args()) {
    size += arg.data().size();
  }
  return size;
}

void CoreWorkerClient::SendRequests() {
  absl::MutexLock lock(&mutex_);
  auto this_ptr = this->shared_from_this();

  while (!send_queue_.empty() && rpc_bytes_in_flight_ < kMaxBytesInFlight) {
    auto pair = std::move(*send_queue_.begin());
    send_queue_.pop_front();

    auto request = std::move(pair.first);
    int64_t task_size = RequestSizeInBytes(*request);
    int64_t seq_no = request->sequence_number();
    request->set_client_processed_up_to(max_finished_seq_no_.value());
    rpc_bytes_in_flight_ += task_size;

    auto rpc_callback =
        [this, this_ptr, seq_no, task_size, callback = std::move(pair.second)](
            Status status, rpc::PushTaskReply &&reply) {
          {
            absl::MutexLock lk(&mutex_);
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

}  // namespace rpc
}  // namespace ray
