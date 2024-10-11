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
#include "absl/strings/str_format.h"
#include "ray/common/grpc_util.h"
#include "ray/rpc/client_call.h"
#include "ray/rpc/grpc_client.h"

namespace ray {
namespace rpc {

#define INVOKE_RETRYABLE_RPC_CALL(retryable_rpc_client,                       \
                                  SERVICE,                                    \
                                  METHOD,                                     \
                                  request,                                    \
                                  callback,                                   \
                                  rpc_client,                                 \
                                  method_timeout_ms)                          \
  (retryable_rpc_client->CallMethod<SERVICE, METHOD##Request, METHOD##Reply>( \
      &SERVICE::Stub::PrepareAsync##METHOD,                                   \
      rpc_client,                                                             \
      #SERVICE ".grpc_client." #METHOD,                                       \
      request,                                                                \
      callback,                                                               \
      method_timeout_ms))

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

class RetryableGrpcClient : public std::enable_shared_from_this<RetryableGrpcClient> {
 public:
  static std::shared_ptr<RetryableGrpcClient> Create(
      std::shared_ptr<grpc::Channel> channel,
      instrumented_io_context &io_context,
      uint64_t max_pending_requests_bytes,
      uint64_t check_channel_status_interval_milliseconds,
      uint64_t server_unavailable_timeout_seconds,
      std::function<void()> server_unavailable_timeout_callback,
      const std::string &server_name) {
    auto retryable_grpc_client = std::shared_ptr<RetryableGrpcClient>(
        new RetryableGrpcClient(channel,
                                io_context,
                                max_pending_requests_bytes,
                                check_channel_status_interval_milliseconds,
                                server_unavailable_timeout_seconds,
                                server_unavailable_timeout_callback,
                                server_name));
    // SetupCheckTimer() MUST be called after we have the shard_ptr of
    // RetryableGrpcClient since it calls shared_from_this()
    // which requires a shared_ptr of this to exist.
    retryable_grpc_client->SetupCheckTimer();
    return retryable_grpc_client;
  }

  RetryableGrpcClient(const RetryableGrpcClient &) = delete;
  RetryableGrpcClient &operator=(const RetryableGrpcClient &) = delete;

  template <typename Service, typename Request, typename Reply>
  void CallMethod(PrepareAsyncFunction<Service, Request, Reply> prepare_async_function,
                  GrpcClient<Service> &grpc_client,
                  const std::string &call_name,
                  const Request &request,
                  const ClientCallback<Reply> &callback,
                  const int64_t timeout_ms) {
    auto executor = new Executor(
        [callback](const ray::Status &status) { callback(status, Reply()); });
    std::weak_ptr<RetryableGrpcClient> weak_self = shared_from_this();
    auto operation_callback = [weak_self, request, callback, executor, timeout_ms](
                                  const ray::Status &status, Reply &&reply) {
      auto self = weak_self.lock();
      if (status.ok() || !IsGrpcRetryableStatus(status) || !self) {
        callback(status, std::move(reply));
        delete executor;
      } else {
        /* In case of transient network error, we queue the request and these requests
         * will be */
        /* executed once network is recovered. */
        self->server_is_unavailable_ = true;
        auto request_bytes = request.ByteSizeLong();
        if (self->pending_requests_bytes_ + request_bytes >
            self->max_pending_requests_bytes_) {
          RAY_LOG(WARNING)
              << "Pending queue for failed request has reached the "
              << "limit. Blocking the current thread until network is recovered";
          while (self->server_is_unavailable_ && !self->shutdown_) {
            self->CheckChannelStatus(false);
            std::this_thread::sleep_for(std::chrono::milliseconds(
                self->check_channel_status_interval_milliseconds_));
          }
          if (self->shutdown_) {
            callback(Status::Disconnected("GRPC client has been disconnected."),
                     std::move(reply));
            delete executor;
          } else {
            executor->Retry();
          }
        } else {
          self->pending_requests_bytes_ += request_bytes;
          auto timeout = timeout_ms == -1 ? absl::InfiniteFuture()
                                          : absl::Now() + absl::Milliseconds(timeout_ms);
          self->pending_requests_.emplace(timeout,
                                          std::make_pair(executor, request_bytes));
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

      // Fail the pending requests.
      while (!pending_requests_.empty()) {
        auto iter = pending_requests_.begin();
        Executor *executor = iter->second.first;
        size_t request_bytes = iter->second.second;
        // Make sure the callback is executed in the io context thread.
        io_context_.post(
            [executor]() {
              executor->Abort(Status::Disconnected("GRPC client has been disconnected."));
              delete executor;
            },
            "RetryableGrpcClient.Shutdown");
        pending_requests_bytes_ -= request_bytes;
        pending_requests_.erase(iter);
      }
    } else {
      RAY_LOG(DEBUG) << "GRPC client has already shutdown.";
    }
  }

  size_t NumPendingRequests() { return pending_requests_.size(); }

  ~RetryableGrpcClient() { Shutdown(); }

 private:
  RetryableGrpcClient(std::shared_ptr<grpc::Channel> channel,
                      instrumented_io_context &io_context,
                      uint64_t max_pending_requests_bytes,
                      uint64_t check_channel_status_interval_milliseconds,
                      uint64_t server_unavailable_timeout_seconds,
                      std::function<void()> server_unavailable_timeout_callback,
                      const std::string &server_name)
      : io_context_(io_context),
        timer_(std::make_unique<boost::asio::deadline_timer>(io_context)),
        channel_(channel),
        max_pending_requests_bytes_(max_pending_requests_bytes),
        check_channel_status_interval_milliseconds_(
            check_channel_status_interval_milliseconds),
        server_unavailable_timeout_seconds_(server_unavailable_timeout_seconds),
        server_unavailable_timeout_callback_(
            std::move(server_unavailable_timeout_callback)),
        server_name_(server_name) {}

  void SetupCheckTimer() {
    auto duration =
        boost::posix_time::milliseconds(check_channel_status_interval_milliseconds_);
    absl::MutexLock lock(&timer_mu_);
    timer_->expires_from_now(duration);
    std::weak_ptr<RetryableGrpcClient> weak_self = shared_from_this();
    timer_->async_wait([weak_self](boost::system::error_code error) {
      if (auto self = weak_self.lock()) {
        if (error == boost::system::errc::success) {
          self->CheckChannelStatus();
        }
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
      executor->Abort(ray::Status::TimedOut(absl::StrFormat(
          "Timed out while waiting for %s to become available.", server_name_)));
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
        uint64_t server_unavailable_duration_seconds = static_cast<uint64_t>(
            absl::ToInt64Seconds(absl::Now() - server_last_available_time_));
        if (server_unavailable_duration_seconds >= server_unavailable_timeout_seconds_) {
          RAY_LOG(WARNING) << server_name_ << " has been unavailable for "
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

  instrumented_io_context &io_context_;
  // Timer can be called from either the GRPC event loop, or the application's
  // main thread. It needs to be protected by a mutex.
  absl::Mutex timer_mu_;
  const std::unique_ptr<boost::asio::deadline_timer> timer_;

  std::shared_ptr<grpc::Channel> channel_;

  uint64_t max_pending_requests_bytes_;
  uint64_t check_channel_status_interval_milliseconds_;
  uint64_t server_unavailable_timeout_seconds_;
  std::function<void()> server_unavailable_timeout_callback_;
  std::string server_name_;

  bool server_is_unavailable_ = false;
  absl::Time server_last_available_time_ = absl::Now();

  std::atomic<bool> shutdown_ = false;
  absl::btree_multimap<absl::Time, std::pair<Executor *, size_t>> pending_requests_;
  size_t pending_requests_bytes_ = 0;
};

}  // namespace rpc
}  // namespace ray
