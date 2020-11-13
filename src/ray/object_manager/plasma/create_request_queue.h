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

#include <memory>
#include <string>
#include <vector>

#include "ray/common/status.h"
#include "ray/object_manager/plasma/common.h"
#include "ray/object_manager/plasma/connection.h"

namespace plasma {

using ray::Status;

using CreateObjectCallback = std::function<Status(bool reply_on_oom, bool evict_if_full)>;

class CreateRequestQueue {
 public:
  CreateRequestQueue(int32_t max_retries,
      bool evict_if_full,
      std::function<void()> on_store_full)
    : max_retries_(max_retries),
      evict_if_full_(evict_if_full),
      on_store_full_(on_store_full) {
        RAY_LOG(DEBUG) << "Starting plasma::CreateRequestQueue with " << max_retries_ << " retries on OOM, evict if full? " << (evict_if_full_ ? 1 : 0);
      }

  /// Add a request to the queue.
  ///
  /// The request may not get tried immediately if the head of the queue is not
  /// serviceable.
  ///
  /// \param client The client that sent the request.
  void AddRequest(const std::shared_ptr<ClientInterface> &client, const CreateObjectCallback &request_callback);

  /// Process requests in the queue.
  ///
  /// This will try to process as many requests in the queue as possible, in
  /// FIFO order. If the first request is not serviceable, this will break and
  /// the caller should try again later.
  ///
  /// \return Bad status for the first request in the queue if it failed to be
  /// serviced, or OK if all requests were fulfilled.
  Status ProcessRequests();

  /// Remove all requests that were made by a client that is now disconnected.
  ///
  /// \param client The client that was disconnected.
  void RemoveDisconnectedClientRequests(const std::shared_ptr<ClientInterface> &client);

 private:
  /// Process a single request. Returns the status returned by the request
  /// handler.
  Status ProcessRequest(const CreateObjectCallback &request_callback);

  /// The maximum number of times to retry each request upon OOM.
  const int32_t max_retries_;

  /// The number of times the request at the head of the queue has been tried.
  int32_t num_retries_ = 0;

  /// On the first attempt to create an object, whether to evict from the
  /// object store to make space. If the first attempt fails, then we will
  /// always try to evict.
  const bool evict_if_full_;

  /// A callback to call if the object store is full.
  const std::function<void()> on_store_full_;

  /// Queue of object creation requests to respond to. Requests will be placed
  /// on this queue if the object store does not have enough room at the time
  /// that the client made the creation request, but space may be made through
  /// object spilling. Once the raylet notifies us that objects have been
  /// spilled, we will attempt to process these requests again and respond to
  /// the client if successful or out of memory. If more objects must be
  /// spilled, the request will be replaced at the head of the queue.
  /// TODO(swang): We should also queue objects here even if there is no room
  /// in the object store. Then, the client does not need to poll on an
  /// OutOfMemory error and we can just respond to them once there is enough
  /// space made, or after a timeout.
  std::list<std::pair<const std::shared_ptr<ClientInterface>, const CreateObjectCallback>> queue_;
};

}  // namespace plasma
