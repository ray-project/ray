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

#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <utility>

#include "absl/container/btree_map.h"
#include "absl/time/time.h"
#include "ray/common/grpc_util.h"
#include "ray/rpc/client_call.h"
#include "ray/rpc/grpc_client.h"

namespace ray::rpc {

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
  class RetryableGrpcRequest {
   public:
    RetryableGrpcRequest(std::function<void(RetryableGrpcRequest)> executor,
                         std::function<void(ray::Status)> failure_callback,
                         size_t request_bytes,
                         int64_t timeout_ms)
        : executor_(std::move(executor)),
          failure_callback_(std::move(failure_callback)),
          request_bytes_(request_bytes),
          timeout_ms_(timeout_ms) {}

    /// This function is used to call the RPC method to send out the request.
    void CallMethod() && { executor_(std::move(*this)); }

    void Fail(const ray::Status &status) { failure_callback_(status); }

    size_t GetRequestBytes() const { return request_bytes_; }

    int64_t GetTimeoutMs() const { return timeout_ms_; }

   private:
    std::function<void(RetryableGrpcRequest request)> executor_;
    std::function<void(ray::Status)> failure_callback_;
    size_t request_bytes_;
    int64_t timeout_ms_;
  };

 public:
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

  RetryableGrpcClient(const RetryableGrpcClient &) = delete;
  RetryableGrpcClient &operator=(const RetryableGrpcClient &) = delete;

  ~RetryableGrpcClient();

  template <typename Service, typename Request, typename Reply>
  void CallMethod(PrepareAsyncFunction<Service, Request, Reply> prepare_async_function,
                  std::shared_ptr<GrpcClient<Service>> grpc_client,
                  std::string call_name,
                  Request request,
                  ClientCallback<Reply> callback,
                  int64_t timeout_ms) {
    auto weak_retryable_grpc_client = weak_from_this();
    auto executor = [weak_retryable_grpc_client = std::move(weak_retryable_grpc_client),
                     prepare_async_function = std::move(prepare_async_function),
                     grpc_client = std::move(grpc_client),
                     call_name = std::move(call_name),
                     request = std::move(request),
                     callback](RetryableGrpcRequest retryable_grpc_request) {
      auto timeout_ms = retryable_grpc_request.GetTimeoutMs();
      grpc_client->template CallMethod<Request, Reply>(
          prepare_async_function,
          request,
          [weak_retryable_grpc_client,
           retryable_grpc_request = std::move(retryable_grpc_request),
           callback = std::move(callback)](const ray::Status &status,
                                           Reply &&reply) mutable {
            auto retryable_grpc_client = weak_retryable_grpc_client.lock();
            if (status.ok() || !IsGrpcRetryableStatus(status) || !retryable_grpc_client) {
              callback(status, std::move(reply));
              return;
            }

            retryable_grpc_client->Retry(std::move(retryable_grpc_request));
          },
          call_name,
          timeout_ms);
    };

    auto failure_callback = [callback](const ray::Status &status) {
      callback(status, Reply{});
    };

    RetryableGrpcRequest(std::move(executor),
                         std::move(failure_callback),
                         request.ByteSizeLong(),
                         timeout_ms)
        .CallMethod();
  }

  void Retry(RetryableGrpcRequest request);

  size_t NumPendingRequests() const { return pending_requests_.size(); }

 private:
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

  // How often the channel status is checked after it's down.
  const uint64_t check_channel_status_interval_milliseconds_;

  // After the server is unavailable for server_unavailable_timeout_seconds_,
  // this callback will be called.
  const uint64_t server_unavailable_timeout_seconds_;
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
  absl::btree_multimap<absl::Time, RetryableGrpcRequest> pending_requests_;

  // Total number of bytes of pending requests.
  size_t pending_requests_bytes_ = 0;
};

}  // namespace ray::rpc
