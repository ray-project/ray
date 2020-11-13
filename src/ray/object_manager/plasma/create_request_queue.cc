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

#include "ray/object_manager/plasma/create_request_queue.h"

#include <stdlib.h>

#include <memory>

#include "ray/object_manager/plasma/common.h"
#include "ray/util/asio_util.h"
#include "ray/util/util.h"

namespace plasma {

void CreateRequestQueue::AddRequest(const std::shared_ptr<ClientInterface> &client, const CreateObjectCallback &request_callback) {
  queue_.push_back({client, request_callback});
}

Status CreateRequestQueue::ProcessRequest(const CreateObjectCallback &request_callback) {
  // Return an OOM error to the client if we have hit the maximum number of
  // retries.
  bool reply_on_oom = (max_retries_ != -1 && num_retries_ >= max_retries_);
  bool evict_if_full = evict_if_full_;
  if (max_retries_ == 0) {
    // If we cannot retry, then always evict on the first attempt.
    evict_if_full = true;
  } else if (num_retries_ > 0) {
    // Always try to evict after the first attempt.
    evict_if_full = true;
  }
  auto status = request_callback(reply_on_oom, evict_if_full);

  if (status.IsTransientObjectStoreFull()) {
    // The object store is full, but we should wait for space to be made
    // through spilling, so do nothing. The caller must guarantee that
    // ProcessRequests is called again so that we can try this request again.
    // NOTE(swang): There could be other requests behind this one that are
    // actually serviceable. This may be inefficient, but eventually this
    // request will get served and unblock the following requests, once
    // enough objects have been spilled.
    // TODO(swang): Ask the raylet to spill enough space for multiple requests
    // at once, instead of just the head of the queue.
    num_retries_ = 0;
  } else if (status.IsObjectStoreFull()) {
    num_retries_++;
    RAY_LOG(DEBUG) << "Not enough memory to create the object, after " << num_retries_ << " tries";
    if (on_store_full_) {
      on_store_full_();
    }
  } else {
    // We have replied to the client.
    num_retries_ = 0;
  }

  return status;
}

Status CreateRequestQueue::ProcessRequests() {
  for (auto request_it = queue_.begin();
      request_it != queue_.end(); ) {
    auto status = ProcessRequest(request_it->second);
    if (!status.ok()) {
      return status;
    }
    request_it = queue_.erase(request_it);
  }
  return Status::OK();
}


void CreateRequestQueue::RemoveDisconnectedClientRequests(const std::shared_ptr<ClientInterface> &client) {
  for (auto it = queue_.begin(); it != queue_.end(); ) {
    if (it->first == client) {
      it = queue_.erase(it);
    } else {
      it++;
    }
  }
}

}  // namespace plasma
