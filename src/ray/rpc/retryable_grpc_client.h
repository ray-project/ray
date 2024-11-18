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
 private:
  class RetryableGrpcRequest : public std::enable_shared_from_this<RetryableGrpcRequest> {
   public:
    template <typename Service, typename Request, typename Reply>
    static std::shared_ptr<RetryableGrpcRequest> Create(
        std::weak_ptr<RetryableGrpcClient> weak_retryable_grpc_client,
        PrepareAsyncFunction<Service, Request, Reply> prepare_async_function,
        std::shared_ptr<GrpcClient<Service>> grpc_client,
        const std::string &call_name,
        const Request &request,
        const ClientCallback<Reply> &callback,
        const int64_t timeout_ms);

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
      const std::string &server_name) {
    return std::shared_ptr<RetryableGrpcClient>(
        new RetryableGrpcClient(channel,
                                io_context,
                                max_pending_requests_bytes,
                                check_channel_status_interval_milliseconds,
                                server_unavailable_timeout_seconds,
                                server_unavailable_timeout_callback,
                                server_name));
  }

  RetryableGrpcClient(const RetryableGrpcClient &) = delete;
  RetryableGrpcClient &operator=(const RetryableGrpcClient &) = delete;

  template <typename Service, typename Request, typename Reply>
  void CallMethod(PrepareAsyncFunction<Service, Request, Reply> prepare_async_function,
                  std::shared_ptr<GrpcClient<Service>> grpc_client,
                  const std::string &call_name,
                  const Request &request,
                  const ClientCallback<Reply> &callback,
                  const int64_t timeout_ms);

  void Retry(std::shared_ptr<RetryableGrpcRequest> request);

  // Return the number of pending requests waiting for retry.
  size_t NumPendingRequests() const { return pending_requests_.size(); }

  ~RetryableGrpcClient();

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
  const std::unique_ptr<boost::asio::deadline_timer> timer_;

  std::shared_ptr<grpc::Channel> channel_;

  const uint64_t max_pending_requests_bytes_;
  const uint64_t check_channel_status_interval_milliseconds_;
  const uint64_t server_unavailable_timeout_seconds_;
  std::function<void()> server_unavailable_timeout_callback_;
  const std::string server_name_;

  // Only accessed by the io_context_ thread, no mutext needed.
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
  size_t pending_requests_bytes_ = 0;
};

template <typename Service, typename Request, typename Reply>
void RetryableGrpcClient::CallMethod(
    PrepareAsyncFunction<Service, Request, Reply> prepare_async_function,
    std::shared_ptr<GrpcClient<Service>> grpc_client,
    const std::string &call_name,
    const Request &request,
    const ClientCallback<Reply> &callback,
    const int64_t timeout_ms) {
  RetryableGrpcRequest::Create(weak_from_this(),
                               prepare_async_function,
                               grpc_client,
                               call_name,
                               request,
                               callback,
                               timeout_ms)
      ->CallMethod();
}

template <typename Service, typename Request, typename Reply>
std::shared_ptr<RetryableGrpcClient::RetryableGrpcRequest>
RetryableGrpcClient::RetryableGrpcRequest::Create(
    std::weak_ptr<RetryableGrpcClient> weak_retryable_grpc_client,
    PrepareAsyncFunction<Service, Request, Reply> prepare_async_function,
    std::shared_ptr<GrpcClient<Service>> grpc_client,
    const std::string &call_name,
    const Request &request,
    const ClientCallback<Reply> &callback,
    const int64_t timeout_ms) {
  RAY_CHECK(callback != nullptr);

  auto executor = [weak_retryable_grpc_client,
                   prepare_async_function,
                   grpc_client,
                   call_name,
                   request,
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
            return;
          }

          retryable_grpc_client->Retry(retryable_grpc_request);
        },
        call_name,
        retryable_grpc_request->GetTimeoutMs());
  };

  auto failure_callback = [callback](const ray::Status &status) {
    callback(status, Reply());
  };

  return std::shared_ptr<RetryableGrpcClient::RetryableGrpcRequest>(
      new RetryableGrpcClient::RetryableGrpcRequest(std::move(executor),
                                                    std::move(failure_callback),
                                                    request.ByteSizeLong(),
                                                    timeout_ms));
}

}  // namespace rpc
}  // namespace ray
