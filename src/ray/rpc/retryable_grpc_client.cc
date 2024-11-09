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
      Executor *executor = iter->second.first;
      // Make sure the callback is executed in the io context thread.
      io_context_.post(
          [executor]() {
            executor->Abort(Status::Disconnected("GRPC client is shut down."));
            delete executor;
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
    if (auto self = weak_self.lock()) {
      if (error == boost::system::errc::success) {
        self->CheckChannelStatus();
      }
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
      auto [executor, request_bytes] = iter->second;
      executor->Abort(ray::Status::TimedOut(absl::StrFormat(
          "Timed out while waiting for %s to become available.", server_name_)));
      pending_requests_bytes_ -= request_bytes;
      delete executor;
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
    if (server_unavailable_timeout_time_ > absl::Now()) {
      RAY_LOG(WARNING) << server_name_ << " has been unavailable for more than "
                       << server_unavailable_timeout_seconds_ << " seconds";
      server_unavailable_timeout_callback_();
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
        pending_requests_.begin()->second.first->Retry();
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
}  // namespace rpc
}  // namespace ray
