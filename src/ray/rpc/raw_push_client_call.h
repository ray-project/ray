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

#include <grpcpp/generic/generic_stub.h>
#include <grpcpp/grpcpp.h>

#include <functional>
#include <memory>
#include <string>
#include <utility>

#include "ray/common/grpc_util.h"
#include "ray/common/status.h"
#include "ray/rpc/client_call.h"

namespace ray {
namespace rpc {

/// A ClientCall implementation for raw ByteBuffer-based Push RPCs.
/// Uses GenericStub to send a pre-serialized ByteBuffer request and
/// receive a ByteBuffer reply (which is empty for PushReply).
class RawPushClientCall : public ClientCall {
 public:
  explicit RawPushClientCall(std::function<void(const Status &)> callback,
                             std::shared_ptr<StatsHandle> stats_handle,
                             int64_t timeout_ms = -1)
      : callback_(std::move(callback)), stats_handle_(std::move(stats_handle)) {
    if (timeout_ms != -1) {
      auto deadline =
          std::chrono::system_clock::now() + std::chrono::milliseconds(timeout_ms);
      context_.set_deadline(deadline);
    }
  }

  Status GetStatus() override {
    absl::MutexLock lock(&mutex_);
    return return_status_;
  }

  void SetReturnStatus() override {
    absl::MutexLock lock(&mutex_);
    return_status_ = GrpcStatusToRayStatus(status_);
  }

  void OnReplyReceived() override {
    ray::Status status;
    {
      absl::MutexLock lock(&mutex_);
      status = return_status_;
    }
    if (callback_ != nullptr) {
      callback_(status);
    }
  }

  std::shared_ptr<StatsHandle> GetStatsHandle() override { return stats_handle_; }

 private:
  grpc::ByteBuffer reply_buffer_;
  std::function<void(const Status &)> callback_;
  std::shared_ptr<StatsHandle> stats_handle_;
  std::unique_ptr<grpc::ClientAsyncResponseReader<grpc::ByteBuffer>> response_reader_;
  grpc::Status status_;
  absl::Mutex mutex_;
  ray::Status return_status_ ABSL_GUARDED_BY(mutex_);
  grpc::ClientContext context_;

  friend class RawPushClientCallManager;
};

/// Manages sending raw ByteBuffer Push requests via GenericStub.
/// Integrates with the existing ClientCallManager completion queue.
class RawPushClientCallManager {
 public:
  RawPushClientCallManager(std::shared_ptr<grpc::Channel> channel,
                           ClientCallManager &call_manager)
      : generic_stub_(std::make_unique<grpc::GenericStub>(channel)),
        call_manager_(call_manager) {}

  /// Send a raw Push request as a ByteBuffer.
  void Push(grpc::ByteBuffer request, std::function<void(const Status &)> callback) {
    auto stats_handle = call_manager_.GetMainService().stats()->RecordStart(
        "ObjectManagerService.grpc_client.Push");

    auto call =
        std::make_shared<RawPushClientCall>(std::move(callback), std::move(stats_handle));

    // Use GenericStub to prepare a unary call with raw ByteBuffer.
    call->response_reader_ =
        generic_stub_->PrepareUnaryCall(&call->context_,
                                        "/ray.rpc.ObjectManagerService/Push",
                                        request,
                                        call_manager_.GetCompletionQueue());
    call->response_reader_->StartCall();

    auto *tag = new ClientCallTag(call);
    call->response_reader_->Finish(
        &call->reply_buffer_, &call->status_, static_cast<void *>(tag));
  }

 private:
  std::unique_ptr<grpc::GenericStub> generic_stub_;
  ClientCallManager &call_manager_;
};

}  // namespace rpc
}  // namespace ray
