// Copyright 2024 The Ray Authors.
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

#include <chrono>

#include "absl/container/btree_map.h"
#include "ray/common/grpc_util.h"
#include "ray/rpc/client_call.h"
#include "ray/rpc/grpc_client.h"

namespace ray {
namespace rpc {

/// \class Executor
/// Executor saves operation and support retries.
class Executor {
 public:
  Executor(std::function<void(const ray::Status &)> abort_callback)
      : abort_callback_(std::move(abort_callback)) {}

  /// This function is used to execute the given operation.
  ///
  /// \param operation The operation to be executed.
  void Execute(std::function<void()> operation) {
    operation_ = std::move(operation);
    operation_();
  }

  /// This function is used to retry the given operation.
  void Retry() { operation_(); }

  void Abort(const ray::Status &status) { abort_callback_(status); }

 private:
  std::function<void(ray::Status)> abort_callback_;
  std::function<void()> operation_;
};

class RetryableGrpcClient {
 public:
  RetryableGrpcClient(std::shared_ptr<grpc::Channel> channel,
                      instrumented_io_context &io_context,
                      uint64_t max_pending_requests_bytes,
                      uint64_t check_channel_status_interval_milliseconds,
                      uint64_t server_unavailable_timeout_seconds,
                      std::function<void()> server_unavailable_timeout_callback)
      : timer_(std::make_unique<boost::asio::deadline_timer>(io_context)),
        channel_(channel),
        max_pending_requests_bytes_(max_pending_requests_bytes),
        check_channel_status_interval_milliseconds_(
            check_channel_status_interval_milliseconds),
        server_unavailable_timeout_seconds_(server_unavailable_timeout_seconds),
        server_unavailable_timeout_callback_(
            std::move(server_unavailable_timeout_callback)) {
    SetupCheckTimer();
  }

  template <typename Service, typename Request, typename Reply>
  void CallMethod(PrepareAsyncFunction<Service, Request, Reply> prepare_async_function,
                  GrpcClient<Service> &grpc_client,
                  const std::string &call_name,
                  const Request &request,
                  const ClientCallback<Reply> &callback,
                  const int64_t timeout_ms) {
    auto executor = new Executor(
        [callback](const ray::Status &status) { callback(status, Reply()); });
    auto operation_callback = [this, request, callback, executor, timeout_ms](
                                  const ray::Status &status, Reply &&reply) {
      if (status.ok()) {
        callback(status, std::move(reply));
        delete executor;
      } else if (!IsGrpcRetryableStatus(status)) {
        callback(status, std::move(reply));
        delete executor;
      } else {
        /* In case of transient network error, we queue the request and these requests
         * will be */
        /* executed once network is recovered. */
        server_is_unavailable_ = true;
        auto request_bytes = request.ByteSizeLong();
        if (pending_requests_bytes_ + request_bytes > max_pending_requests_bytes_) {
          RAY_LOG(WARNING)
              << "Pending queue for failed request has reached the "
              << "limit. Blocking the current thread until network is recovered";
          while (server_is_unavailable_ && !shutdown_) {
            CheckChannelStatus(false);
            std::this_thread::sleep_for(
                std::chrono::milliseconds(check_channel_status_interval_milliseconds_));
          }
          if (shutdown_) {
            callback(Status::Disconnected("GRPC client has been disconnected."),
                     std::move(reply));
            delete executor;
          } else {
            executor->Retry();
          }
        } else {
          pending_requests_bytes_ += request_bytes;
          auto timeout = timeout_ms == -1 ? absl::InfiniteFuture()
                                          : absl::Now() + absl::Milliseconds(timeout_ms);
          pending_requests_.emplace(timeout, std::make_pair(executor, request_bytes));
        }
      }
    };
    auto operation = [prepare_async_function,
                      &grpc_client,
                      call_name,
                      request,
                      operation_callback,
                      timeout_ms]() {
      grpc_client.template CallMethod<Request, Reply>(
          prepare_async_function, request, operation_callback, call_name, timeout_ms);
    };
    executor->Execute(std::move(operation));
  }

  void Shutdown() {
    if (!shutdown_.exchange(true)) {
      // First call to shut down this GRPC client.
      absl::MutexLock lock(&timer_mu_);
      timer_->cancel();
    } else {
      RAY_LOG(DEBUG) << "GRPC client has already shutdown.";
    }
  }

 private:
  void SetupCheckTimer() {
    auto duration =
        boost::posix_time::milliseconds(check_channel_status_interval_milliseconds_);
    absl::MutexLock lock(&timer_mu_);
    timer_->expires_from_now(duration);
    timer_->async_wait([this](boost::system::error_code error) {
      if (error == boost::system::errc::success) {
        CheckChannelStatus();
      }
    });
  }

  void CheckChannelStatus(bool reset_timer = true) {
    if (shutdown_) {
      return;
    }

    auto status = channel_->GetState(false);
    // https://grpc.github.io/grpc/core/md_doc_connectivity-semantics-and-api.html
    // https://grpc.github.io/grpc/core/connectivity__state_8h_source.html
    if (status != GRPC_CHANNEL_READY) {
      RAY_LOG(DEBUG) << "GRPC channel status: " << status;
    }

    // We need to cleanup all the pending requests which are timeout.
    auto now = absl::Now();
    while (!pending_requests_.empty()) {
      auto iter = pending_requests_.begin();
      if (iter->first > now) {
        break;
      }
      auto [executor, request_bytes] = iter->second;
      executor->Abort(ray::Status::TimedOut(
          "Timed out while waiting for server to become available."));
      pending_requests_bytes_ -= request_bytes;
      delete executor;
      pending_requests_.erase(iter);
    }

    switch (status) {
    case GRPC_CHANNEL_TRANSIENT_FAILURE:
    case GRPC_CHANNEL_CONNECTING:
      if (!server_is_unavailable_) {
        server_is_unavailable_ = true;
      } else {
        int64_t server_unavailable_duration_seconds =
            absl::ToInt64Seconds(absl::Now() - server_last_available_time_);
        if (server_unavailable_duration_seconds >= server_unavailable_timeout_seconds_) {
          RAY_LOG(WARNING) << "Server has been unavailable for "
                           << server_unavailable_duration_seconds << " seconds";
          server_unavailable_timeout_callback_();
        }
      }
      break;
    case GRPC_CHANNEL_SHUTDOWN:
      RAY_CHECK(shutdown_) << "Channel shoud never go to this status.";
      break;
    case GRPC_CHANNEL_READY:
    case GRPC_CHANNEL_IDLE:
      server_last_available_time_ = absl::Now();
      server_is_unavailable_ = false;
      // Retry the ones queued.
      while (!pending_requests_.empty()) {
        pending_requests_.begin()->second.first->Retry();
        pending_requests_.erase(pending_requests_.begin());
      }
      pending_requests_bytes_ = 0;
      break;
    default:
      RAY_LOG(FATAL) << "Not covered status: " << status;
    }
    SetupCheckTimer();
  }

  // Timer can be called from either the GCS RPC event loop, or the application's
  // main thread. It needs to be protected by a mutex.
  absl::Mutex timer_mu_;
  const std::unique_ptr<boost::asio::deadline_timer> timer_;

  std::shared_ptr<grpc::Channel> channel_;

  uint64_t max_pending_requests_bytes_;
  uint64_t check_channel_status_interval_milliseconds_;
  uint64_t server_unavailable_timeout_seconds_;
  std::function<void()> server_unavailable_timeout_callback_;

  bool server_is_unavailable_ = false;
  absl::Time server_last_available_time_ = absl::Now();

  std::atomic<bool> shutdown_ = false;
  absl::btree_multimap<absl::Time, std::pair<Executor *, size_t>> pending_requests_;
  size_t pending_requests_bytes_ = 0;
};

}  // namespace rpc
}  // namespace ray
