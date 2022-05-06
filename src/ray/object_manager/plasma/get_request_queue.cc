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

#include "ray/object_manager/plasma/get_request_queue.h"

namespace plasma {

GetRequest::GetRequest(instrumented_io_context &io_context,
                       const std::shared_ptr<ClientInterface> &client,
                       const std::vector<ObjectID> &object_ids,
                       bool is_from_worker,
                       int64_t num_unique_objects_to_wait_for)
    : client(client),
      object_ids(object_ids.begin(), object_ids.end()),
      objects(object_ids.size()),
      num_unique_objects_to_wait_for(num_unique_objects_to_wait_for),
      num_unique_objects_satisfied(0),
      is_from_worker(is_from_worker),
      timer_(io_context) {}

void GetRequest::AsyncWait(
    int64_t timeout_ms,
    std::function<void(const boost::system::error_code &)> on_timeout) {
  RAY_CHECK(!is_removed_);
  // Set an expiry time relative to now.
  timer_.expires_from_now(std::chrono::milliseconds(timeout_ms));
  timer_.async_wait(on_timeout);
}

void GetRequest::CancelTimer() {
  RAY_CHECK(!is_removed_);
  timer_.cancel();
}

void GetRequest::MarkRemoved() {
  RAY_CHECK(!is_removed_);
  is_removed_ = true;
}

bool GetRequest::IsRemoved() const { return is_removed_; }

void GetRequestQueue::AddRequest(const std::shared_ptr<ClientInterface> &client,
                                 const std::vector<ObjectID> &object_ids,
                                 int64_t timeout_ms,
                                 bool is_from_worker) {
  const absl::flat_hash_set<ObjectID> unique_ids(object_ids.begin(), object_ids.end());
  // Create a get request for this object.
  auto get_request = std::make_shared<GetRequest>(
      io_context_, client, object_ids, is_from_worker, unique_ids.size());
  for (const auto &object_id : unique_ids) {
    // Check if this object is already present
    // locally. If so, record that the object is being used and mark it as accounted for.
    auto entry = object_lifecycle_mgr_.GetObject(object_id);
    if (entry && entry->Sealed()) {
      // Update the get request to take into account the present object.
      entry->ToPlasmaObject(&get_request->objects[object_id], /* checksealed */ true);
      get_request->num_unique_objects_satisfied += 1;
      object_satisfied_callback_(object_id, get_request);
    } else {
      // Add a placeholder plasma object to the get request to indicate that the
      // object is not present. This will be parsed by the client. We set the
      // data size to -1 to indicate that the object is not present.
      get_request->objects[object_id].data_size = -1;
      // Add the get request to the relevant data structures.
      object_get_requests_[object_id].push_back(get_request);
    }
  }

  // If all of the objects are present already or if the timeout is 0, return to
  // the client.
  if (get_request->num_unique_objects_satisfied ==
          get_request->num_unique_objects_to_wait_for ||
      timeout_ms == 0) {
    OnGetRequestCompleted(get_request);
  } else if (timeout_ms != -1) {
    // Set a timer that will cause the get request to return to the client. Note
    // that a timeout of -1 is used to indicate that no timer should be set.
    get_request->AsyncWait(timeout_ms,
                           [this, get_request](const boost::system::error_code &ec) {
                             if (ec != boost::asio::error::operation_aborted) {
                               // Timer was not cancelled, take necessary action.
                               OnGetRequestCompleted(get_request);
                             }
                           });
  }
}

void GetRequestQueue::RemoveGetRequestsForClient(
    const std::shared_ptr<ClientInterface> &client) {
  /// TODO: Preventing duplicated can be optimized.
  absl::flat_hash_set<std::shared_ptr<GetRequest>> get_requests_to_remove;
  for (auto const &pair : object_get_requests_) {
    for (const auto &get_request : pair.second) {
      if (get_request->client == client) {
        get_requests_to_remove.insert(get_request);
      }
    }
  }

  // It shouldn't be possible for a given client to be in the middle of multiple get
  // requests.
  RAY_CHECK(get_requests_to_remove.size() <= 1);
  for (const auto &get_request : get_requests_to_remove) {
    RemoveGetRequest(get_request);
  }
}

void GetRequestQueue::RemoveGetRequest(const std::shared_ptr<GetRequest> &get_request) {
  // If the get request is already removed, do no-op. This can happen because the boost
  // timer is not atomic. See https://github.com/ray-project/ray/pull/15071 and
  // https://github.com/ray-project/ray/issues/18400
  if (get_request->IsRemoved()) {
    return;
  }

  // Remove the get request from each of the relevant object_get_requests hash
  // tables if it is present there. It should only be present there if the get
  // request timed out or if it was issued by a client that has disconnected.
  for (const auto &object_id : get_request->object_ids) {
    auto object_request_iter = object_get_requests_.find(object_id);
    if (object_request_iter != object_get_requests_.end()) {
      auto &get_requests = object_request_iter->second;
      // Erase get_request from the vector.
      auto it = std::find(get_requests.begin(), get_requests.end(), get_request);
      if (it != get_requests.end()) {
        get_requests.erase(it);
        // If the vector is empty, remove the object ID from the map.
        if (get_requests.empty()) {
          object_get_requests_.erase(object_request_iter);
        }
      }
    }
  }
  // Remove the get request.
  get_request->CancelTimer();
  get_request->MarkRemoved();
}

void GetRequestQueue::MarkObjectSealed(const ObjectID &object_id) {
  auto it = object_get_requests_.find(object_id);
  // If there are no get requests involving this object, then return.
  if (it == object_get_requests_.end()) {
    return;
  }

  auto &get_requests = it->second;

  // After finishing the loop below, get_requests and it will have been
  // invalidated by the removal of object_id from object_get_requests_.
  size_t index = 0;
  size_t num_requests = get_requests.size();
  for (size_t i = 0; i < num_requests; ++i) {
    auto get_request = get_requests[index];
    auto entry = object_lifecycle_mgr_.GetObject(object_id);
    RAY_CHECK(entry != nullptr);
    entry->ToPlasmaObject(&get_request->objects[object_id], /* check sealed */ true);
    get_request->num_unique_objects_satisfied += 1;
    object_satisfied_callback_(object_id, get_request);
    // If this get request is done, reply to the client.
    if (get_request->num_unique_objects_satisfied ==
        get_request->num_unique_objects_to_wait_for) {
      OnGetRequestCompleted(get_request);
    } else {
      // The call to ReturnFromGet will remove the current element in the
      // array, so we only increment the counter in the else branch.
      index += 1;
    }
  }

  // No get requests should be waiting for this object anymore. The object ID
  // may have been removed from the object_get_requests_ by ReturnFromGet, but
  // if the get request has not returned yet, then remove the object ID from the
  // map here.
  it = object_get_requests_.find(object_id);
  if (it != object_get_requests_.end()) {
    object_get_requests_.erase(object_id);
  }
}

bool GetRequestQueue::IsGetRequestExist(const ObjectID &object_id) {
  return object_get_requests_.contains(object_id);
}

int64_t GetRequestQueue::GetRequestCount(const ObjectID &object_id) {
  return object_get_requests_[object_id].size();
}

void GetRequestQueue::OnGetRequestCompleted(
    const std::shared_ptr<GetRequest> &get_request) {
  all_objects_satisfied_callback_(get_request);
  RemoveGetRequest(get_request);
}
}  // namespace plasma
