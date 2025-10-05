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

#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <utility>

#include "absl/container/btree_map.h"
#include "absl/strings/str_format.h"
#include "absl/time/time.h"
#include "ray/common/grpc_util.h"
#include "ray/rpc/grpc_client.h"
#include "ray/rpc/rpc_callback_types.h"

namespace ray::rpc {

// This macro wraps the logic to call a specific RPC method of a service with the
// retryable grpc client, to make it easier to implement a new RPC client.
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
      std::move(request),                                                     \
      callback,                                                               \
      method_timeout_ms))

// Define a void retryable RPC client method.
#define VOID_RETRYABLE_RPC_CLIENT_METHOD(                                               \
    retryable_rpc_client, SERVICE, METHOD, rpc_client, method_timeout_ms, SPECS)        \
  void METHOD(METHOD##Request &&request, const ClientCallback<METHOD##Reply> &callback) \
      SPECS {                                                                           \
    INVOKE_RETRYABLE_RPC_CALL(retryable_rpc_client,                                     \
                              SERVICE,                                                  \
                              METHOD,                                                   \
                              request,                                                  \
                              callback,                                                 \
                              rpc_client,                                               \
                              method_timeout_ms);                                       \
  }

/**
 * The client makes RPC calls through the provided underlying grpc client.
 * - If the call goes through, the user provided callback is invoked.
 * - If the call fails due to transient network error, it is added to a retry queue.
 * The client waits for the grpc channel reconnection to resend the requests.
 * - If the total number of request bytes in the queue exceeds max_pending_requests_bytes,
 * the io context thread is blocked until some requests are resent.
 * - If a call's timeout_ms reaches during retry, its callback is called with
 * Status::TimedOut.
 * - If the whole client does not reconnect within
 * server_unavailable_timeout_seconds, server_unavailable_timeout_callback is invoked.
 *
 * When all callers of the client release the shared_ptr of the client, the client
 * destructor is called and the client is shut down.
 */
class RetryableGrpcClient : public std::enable_shared_from_this<RetryableGrpcClient> {
 private:
  /**
   * Represents a single retryable grpc request.
   * The lifecycle is managed by shared_ptr and it's either in the callback of an ongoing
   * call or the RetryableGrpcClient retry queue.
   *
   * Implementation wise, it uses std::function for type erasure so that it can represent
   * any underlying grpc request without making this class a template.
   */
  class RetryableGrpcRequest : public std::enable_shared_from_this<RetryableGrpcRequest> {
   public:
    template <typename Service, typename Request, typename Reply>
    static std::shared_ptr<RetryableGrpcRequest> Create(
        std::weak_ptr<RetryableGrpcClient> weak_retryable_grpc_client,
        PrepareAsyncFunction<Service, Request, Reply> prepare_async_function,
        std::shared_ptr<GrpcClient<Service>> grpc_client,
        std::string call_name,
        Request request,
        ClientCallback<Reply> callback,
        int64_t timeout_ms);

    RetryableGrpcRequest(const RetryableGrpcRequest &) = delete;
    RetryableGrpcRequest &operator=(const RetryableGrpcRequest &) = delete;

    /// This function is used to call the RPC method to send out the request.
    void CallMethod() { executor_(shared_from_this()); }

    void Fail(const ray::Status &status) { failure_callback_(status); }

    size_t GetRequestBytes() const { return request_bytes_; }

    int64_t GetTimeoutMs() const { return timeout_ms_; }

   private:
    RetryableGrpcRequest(
        std::function<void(std::shared_ptr<RetryableGrpcRequest> request)> executor,
        std::function<void(ray::Status)> failure_callback,
        size_t request_bytes,
        int64_t timeout_ms)
        : executor_(std::move(executor)),
          failure_callback_(std::move(failure_callback)),
          request_bytes_(request_bytes),
          timeout_ms_(timeout_ms) {}

    std::function<void(std::shared_ptr<RetryableGrpcRequest> request)> executor_;
    std::function<void(ray::Status)> failure_callback_;
    const size_t request_bytes_;
    const int64_t timeout_ms_;
  };

 public:
  static std::shared_ptr<RetryableGrpcClient> Create(
      std::shared_ptr<grpc::Channel> channel,
      instrumented_io_context &io_context,
      uint64_t max_pending_requests_bytes,
      uint64_t check_channel_status_interval_milliseconds,
      uint64_t server_unavailable_timeout_seconds,
      std::function<void()> server_unavailable_timeout_callback,
      std::string server_name) {
    // C++ limitation: std::make_shared cannot be used because std::shared_ptr cannot
    // invoke private constructors.
    return std::shared_ptr<RetryableGrpcClient>(
        new RetryableGrpcClient(std::move(channel),
                                io_context,
                                max_pending_requests_bytes,
                                check_channel_status_interval_milliseconds,
                                server_unavailable_timeout_seconds,
                                std::move(server_unavailable_timeout_callback),
                                std::move(server_name)));
  }

  RetryableGrpcClient(const RetryableGrpcClient &) = delete;
  RetryableGrpcClient &operator=(const RetryableGrpcClient &) = delete;

  template <typename Service, typename Request, typename Reply>
  void CallMethod(PrepareAsyncFunction<Service, Request, Reply> prepare_async_function,
                  std::shared_ptr<GrpcClient<Service>> grpc_client,
                  std::string call_name,
                  Request request,
                  ClientCallback<Reply> callback,
                  int64_t timeout_ms);

  void Retry(std::shared_ptr<RetryableGrpcRequest> request);

  // Return the number of active (pending or inflight) requests.
  size_t NumActiveRequests() const { return num_active_requests_; }

  ~RetryableGrpcClient();

 private:
  RetryableGrpcClient(std::shared_ptr<grpc::Channel> channel,
                      instrumented_io_context &io_context,
                      uint64_t max_pending_requests_bytes,
                      uint64_t check_channel_status_interval_milliseconds,
                      uint64_t server_unavailable_timeout_seconds,
                      std::function<void()> server_unavailable_timeout_callback,
                      std::string server_name)
      : io_context_(io_context),
        timer_(io_context),
        channel_(std::move(channel)),
        max_pending_requests_bytes_(max_pending_requests_bytes),
        check_channel_status_interval_milliseconds_(
            check_channel_status_interval_milliseconds),
        server_unavailable_timeout_seconds_(server_unavailable_timeout_seconds),
        server_unavailable_timeout_callback_(
            std::move(server_unavailable_timeout_callback)),
        server_name_(std::move(server_name)) {}

  // Set up the timer to run CheckChannelStatus.
  void SetupCheckTimer();

  void CheckChannelStatus(bool reset_timer = true);

  instrumented_io_context &io_context_;
  boost::asio::deadline_timer timer_;

  std::shared_ptr<grpc::Channel> channel_;

  // Max total bytes of pending requests before
  // we pause the io context thread, this is mainly
  // to prevent OOM.
  const uint64_t max_pending_requests_bytes_;
  const uint64_t check_channel_status_interval_milliseconds_;
  const uint64_t server_unavailable_timeout_seconds_;
  // After the server is unavailable for server_unavailable_timeout_seconds_,
  // this callback will be called.
  std::function<void()> server_unavailable_timeout_callback_;
  // Human readable server name for logging purpose.
  const std::string server_name_;

  // This is only set when there are pending requests and
  // we need to check channel status.
  // This is the time when the server will timeout for
  // unavailability and server_unavailable_timeout_callback_
  // will be called.
  std::optional<absl::Time> server_unavailable_timeout_time_;

  // Key is when the request will timeout and value is the request.
  // This is only accessed in the io context thread and the destructor so
  // no mutex is needed.
  absl::btree_multimap<absl::Time, std::shared_ptr<RetryableGrpcRequest>>
      pending_requests_;
  // Total number of bytes of pending requests.
  size_t pending_requests_bytes_ = 0;
  // TODO(57156): this is messy to leave in the retryable grpc client, refactor this
  // Total number of inflight requests.
  std::atomic<size_t> num_active_requests_ = 0;
};

template <typename Service, typename Request, typename Reply>
void RetryableGrpcClient::CallMethod(
    PrepareAsyncFunction<Service, Request, Reply> prepare_async_function,
    std::shared_ptr<GrpcClient<Service>> grpc_client,
    std::string call_name,
    Request request,
    ClientCallback<Reply> callback,
    int64_t timeout_ms) {
  num_active_requests_++;
  RetryableGrpcRequest::Create(weak_from_this(),
                               std::move(prepare_async_function),
                               std::move(grpc_client),
                               std::move(call_name),
                               std::move(request),
                               std::move(callback),
                               timeout_ms)
      ->CallMethod();
}

template <typename Service, typename Request, typename Reply>
std::shared_ptr<RetryableGrpcClient::RetryableGrpcRequest>
RetryableGrpcClient::RetryableGrpcRequest::Create(
    std::weak_ptr<RetryableGrpcClient> weak_retryable_grpc_client,
    PrepareAsyncFunction<Service, Request, Reply> prepare_async_function,
    std::shared_ptr<GrpcClient<Service>> grpc_client,
    std::string call_name,
    Request request,
    ClientCallback<Reply> callback,
    int64_t timeout_ms) {
  RAY_CHECK(callback != nullptr);
  RAY_CHECK(grpc_client.get() != nullptr);

  const auto request_bytes = request.ByteSizeLong();

  auto executor = [weak_retryable_grpc_client = std::move(weak_retryable_grpc_client),
                   prepare_async_function = std::move(prepare_async_function),
                   grpc_client = std::move(grpc_client),
                   call_name = std::move(call_name),
                   request = std::move(request),
                   callback](std::shared_ptr<RetryableGrpcClient::RetryableGrpcRequest>
                                 retryable_grpc_request) {
    grpc_client->template CallMethod<Request, Reply>(
        prepare_async_function,
        request,
        [weak_retryable_grpc_client, retryable_grpc_request, callback](
            const ray::Status &status, Reply &&reply) {
          auto retryable_grpc_client = weak_retryable_grpc_client.lock();
          if (status.ok() || !IsGrpcRetryableStatus(status) || !retryable_grpc_client) {
            callback(status, std::move(reply));
            if (retryable_grpc_client) {
              retryable_grpc_client->num_active_requests_--;
            }
            return;
          }
          retryable_grpc_client->Retry(retryable_grpc_request);
        },
        call_name,
        retryable_grpc_request->GetTimeoutMs());
  };

  auto failure_callback = [weak_retryable_grpc_client,
                           callback](const ray::Status &status) {
    callback(status, Reply{});
    auto retryable_grpc_client = weak_retryable_grpc_client.lock();
    if (retryable_grpc_client) {
      retryable_grpc_client->num_active_requests_--;
    }
  };

  return std::shared_ptr<RetryableGrpcClient::RetryableGrpcRequest>(
      // C++ limitation: std::make_shared cannot be used because std::shared_ptr cannot
      // invoke private constructors.
      new RetryableGrpcClient::RetryableGrpcRequest(
          std::move(executor), std::move(failure_callback), request_bytes, timeout_ms));
}

}  // namespace ray::rpc
