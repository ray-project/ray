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

namespace ray {
namespace raylet {

void WaitManager::Wait(const std::vector<ObjectID> &object_ids, int64_t timeout_ms,
                       uint64_t num_required_objects, const WaitCallback &callback) {
  RAY_CHECK(timeout_ms >= 0 || timeout_ms == -1);
  RAY_CHECK(num_required_objects != 0);
  RAY_CHECK(num_required_objects <= object_ids.size())
      << num_required_objects << " " << object_ids.size();

  const uint64_t wait_id = next_wait_id_++;
  wait_requests_.emplace(wait_id, WaitRequest(*io_service_, timeout_ms, callback,
                                              object_ids, num_required_objects));

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
    auto timeout = boost::posix_time::milliseconds(wait_request.timeout_ms);
    wait_request.timeout_timer.expires_from_now(timeout);
    wait_request.timeout_timer.async_wait(
        [this, wait_id](const boost::system::error_code &error_code) {
          if (error_code.value() != 0) {
            return;
          }
          if (wait_requests_.find(wait_id) == wait_requests_.end()) {
            // When HandleObjectLocal is called first, WaitComplete might be
            // called. The timer may at the same time goes off and may be an
            // interruption will post WaitComplete to io_service_ the second time.
            // This check will avoid the duplicated call of this function.
            return;
          }
          WaitComplete(wait_id);
        });
  }
}

void WaitManager::WaitComplete(uint64_t wait_id) {
  auto iter = wait_requests_.find(wait_id);
  RAY_CHECK(iter != wait_requests_.end());
  auto &wait_request = iter->second;
  // Cancel the timer. This is okay even if the timer hasn't been started.
  // The timer handler will be given a non-zero error code. The handler
  // will do nothing on non-zero error codes.
  wait_request.timeout_timer.cancel();

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
    auto wait_request_iter = wait_requests_.find(wait_id);
    RAY_CHECK(wait_request_iter != wait_requests_.end());
    auto &wait_request = wait_request_iter->second;
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