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

Status CreateRequestQueue::ProcessRequests() {
  for (auto request_it = queue_.begin();
      request_it != queue_.end(); ) {
    auto status = request_it->second();
    if (status.IsTransientObjectStoreFull()) {
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
