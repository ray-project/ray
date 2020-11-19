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

using CreateObjectCallback = std::function<Status()>;

class CreateRequestQueue {
 public:
  CreateRequestQueue() {}

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
