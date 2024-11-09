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

// Define a void retryable RPC client method.
#define VOID_RETRYABLE_RPC_CLIENT_METHOD(                                        \
    retryable_rpc_client, SERVICE, METHOD, rpc_client, method_timeout_ms, SPECS) \
  void METHOD(const METHOD##Request &request,                                    \
              const ClientCallback<METHOD##Reply> &callback) SPECS {             \
    retryable_rpc_client->CallMethod<SERVICE, METHOD##Request, METHOD##Reply>(   \
        &SERVICE::Stub::PrepareAsync##METHOD,                                    \
        rpc_client,                                                              \
        #SERVICE ".grpc_client." #METHOD,                                        \
        request,                                                                 \
        callback,                                                                \
        method_timeout_ms);                                                      \
  }

/// \class Executor
/// Executor saves operation and support retries.
class Executor {
 public:
  explicit Executor(std::function<void(const ray::Status &)> abort_callback)
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

/**
 * The client makes RPC calls through the provided underlying grpc client.
 * If the call goes through, the user provided callback is invoked.
 * If the call fails due to transient network error, it is added to a retry queue.
 * The client waits for the grpc channel reconnection to resend the requests.
 * If the total number of request bytes in the queue exceeds max_pending_requests_bytes,
 * the thread is blocked until some requests are resent.
 * If a call's timeout_ms reaches during retry, its callback is called with
 * Status::TimedOut. If the whole client does not reconnect within
 * server_unavailable_timeout_seconds, server_unavailable_timeout_callback is invoked.
 * When all callers of the client release the shared_ptr of the client, the client
 * destructor is called and the client is shut down.
 */
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
    std::weak_ptr<RetryableGrpcClient> weak_self = weak_from_this();
    auto operation_callback = [weak_self, request, callback, executor, timeout_ms](
                                  const ray::Status &status, Reply &&reply) {
      auto self = weak_self.lock();
      if (status.ok() || !IsGrpcRetryableStatus(status) || !self) {
        callback(status, std::move(reply));
        delete executor;
        return;
      }

      // In case of transient network error, we queue the request and these requests
      // will be executed once network is recovered.
      auto request_bytes = request.ByteSizeLong();
      if (self->pending_requests_bytes_ + request_bytes >
          self->max_pending_requests_bytes_) {
        RAY_LOG(WARNING)
            << "Pending queue for failed request has reached the "
            << "limit. Blocking the current thread until network is recovered";
        if (!self->server_unavailable_timeout_time_.has_value()) {
          self->server_unavailable_timeout_time_ =
              absl::Now() + absl::Seconds(self->server_unavailable_timeout_seconds_);
        }
        while (self->server_unavailable_timeout_time_.has_value() && !self->shutdown_) {
          self->CheckChannelStatus(false);
          std::this_thread::sleep_for(std::chrono::milliseconds(
              self->check_channel_status_interval_milliseconds_));
        }
        if (self->shutdown_) {
          callback(Status::Disconnected("GRPC client has been shutdown."), Reply());
          delete executor;
        } else {
          executor->Retry();
        }
        return;
      }

      bool shutdown = false;
      {
        absl::MutexLock lock(&self->mu_);
        if (!self->shutdown_) {
          self->pending_requests_bytes_ += request_bytes;
          auto timeout = timeout_ms == -1 ? absl::InfiniteFuture()
                                          : absl::Now() + absl::Milliseconds(timeout_ms);
          self->pending_requests_.emplace(timeout,
                                          std::make_pair(executor, request_bytes));
        } else {
          shutdown = true;
        }
      }
      if (shutdown) {
        // Run the callback outside of the lock.
        callback(Status::Disconnected("GRPC client has been shutdown."), Reply());
        delete executor;
      } else if (!self->server_unavailable_timeout_time_.has_value()) {
        self->server_unavailable_timeout_time_ =
            absl::Now() + absl::Seconds(self->server_unavailable_timeout_seconds_);
        self->SetupCheckTimer();
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

  void Shutdown();

  // Return the number of pending requests waiting for retry.
  size_t NumPendingRequests() const {
    absl::MutexLock lock(&mu_);
    return pending_requests_.size();
  }

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
        channel_(std::move(channel)),
        max_pending_requests_bytes_(max_pending_requests_bytes),
        check_channel_status_interval_milliseconds_(
            check_channel_status_interval_milliseconds),
        server_unavailable_timeout_seconds_(server_unavailable_timeout_seconds),
        server_unavailable_timeout_callback_(
            std::move(server_unavailable_timeout_callback)),
        server_name_(server_name) {}

  void SetupCheckTimer();

  void CheckChannelStatus(bool reset_timer = true);

  instrumented_io_context &io_context_;
  mutable absl::Mutex mu_;
  // Timer can be called from either the io_context_ thread, or the application's
  // main thread. It needs to be protected by a mutex.
  const std::unique_ptr<boost::asio::deadline_timer> timer_ ABSL_GUARDED_BY(mu_);

  std::shared_ptr<grpc::Channel> channel_;

  const uint64_t max_pending_requests_bytes_;
  const uint64_t check_channel_status_interval_milliseconds_;
  const uint64_t server_unavailable_timeout_seconds_;
  std::function<void()> server_unavailable_timeout_callback_;
  const std::string server_name_;

  // Only accessed by the io_context_ thread, no mutext needed.
  // This is only set when there are pending requests and
  // we need to check channel status.
  std::optional<absl::Time> server_unavailable_timeout_time_;

  std::atomic<bool> shutdown_ = false;
  // Pending requests can be accessed from either the io_context_ thread, or the
  // application's main thread (e.g. Shutdown()). It needs to be protected by a mutex.
  absl::btree_multimap<absl::Time, std::pair<Executor *, size_t>> pending_requests_
      ABSL_GUARDED_BY(mu_);
  size_t pending_requests_bytes_ = 0;
};

}  // namespace rpc
}  // namespace ray
