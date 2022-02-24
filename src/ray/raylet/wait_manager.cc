// Copyright 2022 The Ray Authors.
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

#include "ray/raylet/wait_manager.h"

#include "ray/util/container_util.h"

namespace ray {
namespace raylet {

void WaitManager::Wait(const std::vector<ObjectID> &object_ids, int64_t timeout_ms,
                       uint64_t num_required_objects, const WaitCallback &callback) {
  RAY_CHECK(timeout_ms >= 0 || timeout_ms == -1);
  RAY_CHECK_NE(num_required_objects, 0u);
  RAY_CHECK_LE(num_required_objects, object_ids.size());

  const uint64_t wait_id = next_wait_id_++;
  wait_requests_.emplace(
      wait_id, WaitRequest(timeout_ms, callback, object_ids, num_required_objects));

  auto &wait_request = wait_requests_.at(wait_id);
  for (const auto &object_id : object_ids) {
    if (is_object_local_(object_id)) {
      wait_request.ready.emplace(object_id);
    }
  }

  for (const auto &object_id : wait_request.object_ids) {
    object_to_wait_requests_[object_id].emplace(wait_id);
  }

  if (wait_request.ready.size() >= wait_request.num_required_objects ||
      wait_request.timeout_ms == 0) {
    // Requirements already satisfied.
    WaitComplete(wait_id);
  } else if (wait_request.timeout_ms != -1) {
    // If a timeout was provided, then set a timer. If there are no
    // enough locally available objects by the time the timer expires,
    // then we will return from the Wait.
    delay_executor_(
        [this, wait_id]() {
          if (wait_requests_.find(wait_id) == wait_requests_.end()) {
            // The wait is already complete by the time the timer expires,
            // so we don't need to do anything here.
            return;
          }
          WaitComplete(wait_id);
        },
        wait_request.timeout_ms);
  }
}

void WaitManager::WaitComplete(uint64_t wait_id) {
  auto &wait_request = map_find_or_die(wait_requests_, wait_id);

  for (const auto &object_id : wait_request.object_ids) {
    auto &requests = object_to_wait_requests_.at(object_id);
    requests.erase(wait_id);
    if (requests.empty()) {
      object_to_wait_requests_.erase(object_id);
    }
  }

  // Order objects according to input order.
  std::vector<ObjectID> ready;
  std::vector<ObjectID> remaining;
  for (const auto &object_id : wait_request.object_ids) {
    if (ready.size() < wait_request.num_required_objects &&
        wait_request.ready.count(object_id) > 0) {
      ready.push_back(object_id);
    } else {
      remaining.push_back(object_id);
    }
  }
  wait_request.callback(ready, remaining);
  wait_requests_.erase(wait_id);
  RAY_LOG(DEBUG) << "Wait request " << wait_id << " finished: ready " << ready.size()
                 << " remaining " << remaining.size();
}

void WaitManager::HandleObjectLocal(const ray::ObjectID &object_id) {
  if (object_to_wait_requests_.find(object_id) == object_to_wait_requests_.end()) {
    return;
  }

  std::vector<uint64_t> complete_waits;
  for (const auto &wait_id : object_to_wait_requests_.at(object_id)) {
    auto &wait_request = map_find_or_die(wait_requests_, wait_id);
    wait_request.ready.emplace(object_id);
    if (wait_request.ready.size() >= wait_request.num_required_objects) {
      complete_waits.emplace_back(wait_id);
    }
  }
  for (const auto &wait_id : complete_waits) {
    WaitComplete(wait_id);
  }
}

std::string WaitManager::DebugString() const {
  std::stringstream ss;
  ss << "WaitManager:";
  ss << "\n- num active wait requests: " << wait_requests_.size();
  return ss.str();
}

}  // namespace raylet
}  // namespace ray
