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

#include "ray/rpc/retryable_grpc_client.h"

namespace ray {
namespace rpc {
void RetryableGrpcClient::Shutdown() {
  absl::MutexLock lock(&mu_);
  if (!shutdown_.exchange(true)) {
    // First call to shut down this GRPC client.
    timer_->cancel();

    // Fail the pending requests.
    while (!pending_requests_.empty()) {
      auto iter = pending_requests_.begin();
      // Make sure the callback is executed in the io context thread.
      io_context_.post(
          [request = std::move(iter->second)]() {
            request->Fail(Status::Disconnected("GRPC client is shut down."));
          },
          "RetryableGrpcClient.Shutdown");
      pending_requests_.erase(iter);
    }
    pending_requests_bytes_ = 0;
  } else {
    RAY_LOG(DEBUG) << "GRPC client has already been shut down.";
  }
}

void RetryableGrpcClient::SetupCheckTimer() {
  auto duration =
      boost::posix_time::milliseconds(check_channel_status_interval_milliseconds_);
  absl::MutexLock lock(&mu_);
  timer_->expires_from_now(duration);
  std::weak_ptr<RetryableGrpcClient> weak_self = weak_from_this();
  timer_->async_wait([weak_self](boost::system::error_code error) {
    if (auto self = weak_self.lock(); self && (error == boost::system::errc::success)) {
      self->CheckChannelStatus();
    }
  });
}

void RetryableGrpcClient::CheckChannelStatus(bool reset_timer) {
  if (shutdown_) {
    return;
  }

  {
    absl::MutexLock lock(&mu_);
    // We need to cleanup all the pending requests which are timeout.
    const auto now = absl::Now();
    while (!pending_requests_.empty()) {
      auto iter = pending_requests_.begin();
      if (iter->first > now) {
        break;
      }
      iter->second->Fail(ray::Status::TimedOut(absl::StrFormat(
          "Timed out while waiting for %s to become available.", server_name_)));
      pending_requests_bytes_ -= iter->second->GetRequestBytes();
      pending_requests_.erase(iter);
    }

    if (pending_requests_.empty()) {
      server_unavailable_timeout_time_ = std::nullopt;
      return;
    }
  }

  auto status = channel_->GetState(false);
  // https://grpc.github.io/grpc/core/md_doc_connectivity-semantics-and-api.html
  // https://grpc.github.io/grpc/core/connectivity__state_8h_source.html
  if (status != GRPC_CHANNEL_READY) {
    RAY_LOG(DEBUG) << "GRPC channel status: " << status;
  }

  switch (status) {
  case GRPC_CHANNEL_TRANSIENT_FAILURE:
  case GRPC_CHANNEL_CONNECTING: {
    if (server_unavailable_timeout_time_ < absl::Now()) {
      RAY_LOG(WARNING) << server_name_ << " has been unavailable for more than "
                       << server_unavailable_timeout_seconds_ << " seconds";
      server_unavailable_timeout_callback_();
      // Reset the unavailable timeout.
      server_unavailable_timeout_time_ =
          absl::Now() + absl::Seconds(server_unavailable_timeout_seconds_);
    }

    if (reset_timer) {
      SetupCheckTimer();
    }

    break;
  }
  case GRPC_CHANNEL_SHUTDOWN: {
    RAY_CHECK(shutdown_) << "Channel shoud never go to this status.";
    break;
  }
  case GRPC_CHANNEL_READY:
  case GRPC_CHANNEL_IDLE: {
    server_unavailable_timeout_time_ = std::nullopt;
    {
      absl::MutexLock lock(&mu_);
      // Retry the ones queued.
      while (!pending_requests_.empty()) {
        pending_requests_.begin()->second->CallMethod();
        pending_requests_.erase(pending_requests_.begin());
      }
      pending_requests_bytes_ = 0;
    }
    break;
  }
  default: {
    RAY_LOG(FATAL) << "Not covered status: " << status;
  }
  }
}

void RetryableGrpcClient::Retry(std::shared_ptr<RetryableGrpcRequest> request) {
  // In case of transient network error, we queue the request and these requests
  // will be executed once network is recovered.
  auto request_bytes = request->GetRequestBytes();
  if (pending_requests_bytes_ + request_bytes > max_pending_requests_bytes_) {
    RAY_LOG(WARNING) << "Pending queue for failed request has reached the "
                     << "limit. Blocking the current thread until network is recovered";
    if (!server_unavailable_timeout_time_.has_value()) {
      server_unavailable_timeout_time_ =
          absl::Now() + absl::Seconds(server_unavailable_timeout_seconds_);
    }
    while (server_unavailable_timeout_time_.has_value() && !shutdown_) {
      CheckChannelStatus(false);
      std::this_thread::sleep_for(
          std::chrono::milliseconds(check_channel_status_interval_milliseconds_));
    }
    if (shutdown_) {
      request->Fail(Status::Disconnected("GRPC client has been shutdown."));
    } else {
      request->CallMethod();
    }
    return;
  }

  bool shutdown = false;
  {
    absl::MutexLock lock(&mu_);
    if (!shutdown_) {
      pending_requests_bytes_ += request_bytes;
      auto timeout = request->GetTimeoutMs() == -1
                         ? absl::InfiniteFuture()
                         : absl::Now() + absl::Milliseconds(request->GetTimeoutMs());
      pending_requests_.emplace(timeout, request);
    } else {
      shutdown = true;
    }
  }
  if (shutdown) {
    // Run the callback outside of the lock.
    request->Fail(Status::Disconnected("GRPC client has been shutdown."));
  } else if (!server_unavailable_timeout_time_.has_value()) {
    server_unavailable_timeout_time_ =
        absl::Now() + absl::Seconds(server_unavailable_timeout_seconds_);
    SetupCheckTimer();
  }
}
}  // namespace rpc
}  // namespace ray
