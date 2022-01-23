// Copyright 2020-2021 The Ray Authors.
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

#include "ray/object_manager/pull_manager.h"

#include "ray/common/common_protocol.h"
#include "ray/stats/metric_defs.h"

namespace ray {

PullManager::PullManager(
    NodeID &self_node_id, const std::function<bool(const ObjectID &)> object_is_local,
    const std::function<void(const ObjectID &, const NodeID &)> send_pull_request,
    const std::function<void(const ObjectID &)> cancel_pull_request,
    const std::function<void(const ObjectID &)> fail_pull_request,
    const RestoreSpilledObjectCallback restore_spilled_object,
    const std::function<double()> get_time_seconds, int pull_timeout_ms,
    int64_t num_bytes_available,
    std::function<std::unique_ptr<RayObject>(const ObjectID &)> pin_object,
    std::function<std::string(const ObjectID &)> get_locally_spilled_object_url)
    : self_node_id_(self_node_id),
      object_is_local_(object_is_local),
      send_pull_request_(send_pull_request),
      cancel_pull_request_(cancel_pull_request),
      restore_spilled_object_(restore_spilled_object),
      get_time_seconds_(get_time_seconds),
      pull_timeout_ms_(pull_timeout_ms),
      num_bytes_available_(num_bytes_available),
      pin_object_(pin_object),
      get_locally_spilled_object_url_(get_locally_spilled_object_url),
      fail_pull_request_(fail_pull_request),
      gen_(std::chrono::high_resolution_clock::now().time_since_epoch().count()) {}

uint64_t PullManager::Pull(const std::vector<rpc::ObjectReference> &object_ref_bundle,
                           BundlePriority prio,
                           std::vector<rpc::ObjectReference> *objects_to_locate) {
  // To avoid edge cases dealing with duplicated object ids in the bundle,
  // canonicalize the set up-front by dropping all duplicates.
  absl::flat_hash_set<ObjectID> seen;
  std::vector<rpc::ObjectReference> deduplicated;
  for (const auto &ref : object_ref_bundle) {
    const auto &id = ObjectRefToId(ref);
    if (seen.count(id) == 0) {
      seen.insert(id);
      deduplicated.push_back(ref);
    }
  }
  Queue::iterator bundle_it;
  if (prio == BundlePriority::GET_REQUEST) {
    bundle_it =
        get_request_bundles_.emplace(next_req_id_++, std::move(deduplicated)).first;
  } else if (prio == BundlePriority::WAIT_REQUEST) {
    bundle_it =
        wait_request_bundles_.emplace(next_req_id_++, std::move(deduplicated)).first;
  } else {
    RAY_CHECK(prio == BundlePriority::TASK_ARGS);
    bundle_it =
        task_argument_bundles_.emplace(next_req_id_++, std::move(deduplicated)).first;
  }
  RAY_LOG(DEBUG) << "Start pull request " << bundle_it->first
                 << ". Bundle size: " << bundle_it->second.objects.size();

  for (const auto &ref : deduplicated) {
    auto obj_id = ObjectRefToId(ref);
    auto it = object_pull_requests_.find(obj_id);
    if (it == object_pull_requests_.end()) {
      RAY_LOG(DEBUG) << "Pull of object " << obj_id;
      // We don't have an active pull for this object yet. Ask the caller to
      // send us notifications about the object's location.
      objects_to_locate->push_back(ref);
      // The first pull request doesn't need to be special case. Instead we can just let
      // the retry timer fire immediately.
      it = object_pull_requests_
               .emplace(obj_id, ObjectPullRequest(/*next_pull_time=*/get_time_seconds_()))
               .first;
    } else {
      if (it->second.object_size_set) {
        bundle_it->second.RegisterObjectSize(it->second.object_size);
      }
    }
    it->second.bundle_request_ids.insert(bundle_it->first);
  }

  // We have a new request. Activate the new request, if the
  // current available memory allows it.
  UpdatePullsBasedOnAvailableMemory(num_bytes_available_);

  return bundle_it->first;
}

bool PullManager::ActivateNextPullBundleRequest(const Queue &bundles,
                                                uint64_t *highest_req_id_being_pulled,
                                                bool respect_quota,
                                                std::vector<ObjectID> *objects_to_pull) {
  // Get the next pull request in the queue.
  const auto last_request_it = bundles.find(*highest_req_id_being_pulled);
  auto next_request_it = last_request_it;
  if (next_request_it == bundles.end()) {
    // No requests are active. Get the first request in the queue.
    next_request_it = bundles.begin();
  } else {
    next_request_it++;
  }

  if (next_request_it == bundles.end()) {
    // No requests in the queue.
    return false;
  }

  if (next_request_it->second.num_object_sizes_missing > 0) {
    // There is at least one object size missing. We should not activate the
    // bundle, since it may put us over the available capacity.
    return false;
  }

  // Activate the pull bundle request if possible.
  {
    absl::MutexLock lock(&active_objects_mu_);

    // First calculate the bytes we need.
    int64_t bytes_to_pull = 0;
    for (const auto &ref : next_request_it->second.objects) {
      auto obj_id = ObjectRefToId(ref);
      bool needs_pull = active_object_pull_requests_.count(obj_id) == 0;
      if (needs_pull) {
        // This is the first bundle request in the queue to require this object.
        // Add the size to the number of bytes being pulled.
        auto it = object_pull_requests_.find(obj_id);
        RAY_CHECK(it != object_pull_requests_.end());
        // TODO(ekl) this overestimates bytes needed if it's already available
        // locally.
        bytes_to_pull += it->second.object_size;
      }
    }

    // Quota check.
    if (respect_quota && num_active_bundles_ >= 1 && bytes_to_pull > RemainingQuota()) {
      RAY_LOG(DEBUG) << "Bundle would exceed quota: "
                     << "num_bytes_being_pulled(" << num_bytes_being_pulled_
                     << ") + "
                        "bytes_to_pull("
                     << bytes_to_pull
                     << ") - "
                        "pinned_objects_size("
                     << pinned_objects_size_
                     << ") > "
                        "num_bytes_available("
                     << num_bytes_available_ << ")";
      return false;
    }

    RAY_LOG(DEBUG) << "Activating request " << next_request_it->first
                   << " num bytes being pulled: " << num_bytes_being_pulled_
                   << " num bytes available: " << num_bytes_available_;
    num_bytes_being_pulled_ += bytes_to_pull;
    for (const auto &ref : next_request_it->second.objects) {
      auto obj_id = ObjectRefToId(ref);
      bool needs_pull = active_object_pull_requests_.count(obj_id) == 0;
      active_object_pull_requests_[obj_id].insert(next_request_it->first);
      if (needs_pull) {
        RAY_LOG(DEBUG) << "Activating pull for object " << obj_id;
        TryPinObject(obj_id);
        objects_to_pull->push_back(obj_id);
        ResetRetryTimer(obj_id);
      }
    }
  }

  // Update the pointer to the last pull request that we are actively pulling.
  RAY_CHECK(next_request_it->first > *highest_req_id_being_pulled);
  *highest_req_id_being_pulled = next_request_it->first;

  num_active_bundles_ += 1;
  return true;
}

void PullManager::DeactivatePullBundleRequest(
    const Queue &bundles, const Queue::iterator &request_it,
    uint64_t *highest_req_id_being_pulled,
    std::unordered_set<ObjectID> *objects_to_cancel) {
  for (const auto &ref : request_it->second.objects) {
    absl::MutexLock lock(&active_objects_mu_);
    auto obj_id = ObjectRefToId(ref);
    auto it = active_object_pull_requests_.find(obj_id);
    if (it == active_object_pull_requests_.end() ||
        !it->second.erase(request_it->first)) {
      // The object is already deactivated, no action is required.
      continue;
    }
    if (it->second.empty()) {
      RAY_LOG(DEBUG) << "Deactivating pull for object " << obj_id;
      auto it = object_pull_requests_.find(obj_id);
      RAY_CHECK(it != object_pull_requests_.end());
      num_bytes_being_pulled_ -= it->second.object_size;
      active_object_pull_requests_.erase(it->first);
      UnpinObject(it->first);
      objects_to_cancel->insert(obj_id);
    }
  }

  // If this was the last active request, update the pointer to its
  // predecessor, if one exists.
  if (*highest_req_id_being_pulled == request_it->first) {
    if (request_it == bundles.begin()) {
      *highest_req_id_being_pulled = 0;
    } else {
      *highest_req_id_being_pulled = std::prev(request_it)->first;
    }
  }

  num_active_bundles_ -= 1;
}

void PullManager::DeactivateUntilMarginAvailable(
    const std::string &debug_name, Queue &bundles, int retain_min, int64_t quota_margin,
    uint64_t *highest_id_for_bundle, std::unordered_set<ObjectID> *object_ids_to_cancel) {
  while (RemainingQuota() < quota_margin && *highest_id_for_bundle != 0) {
    if (num_active_bundles_ <= retain_min) {
      return;
    }
    RAY_LOG(DEBUG) << "Deactivating " << debug_name << " " << *highest_id_for_bundle
                   << " num bytes being pulled: " << num_bytes_being_pulled_
                   << " num bytes available: " << num_bytes_available_;
    const auto last_request_it = bundles.find(*highest_id_for_bundle);
    RAY_CHECK(last_request_it != bundles.end());
    DeactivatePullBundleRequest(bundles, last_request_it, highest_id_for_bundle,
                                object_ids_to_cancel);
  }
}

int64_t PullManager::RemainingQuota() {
  // Note that plasma counts pinned bytes as used.
  int64_t bytes_left_to_pull = num_bytes_being_pulled_ - pinned_objects_size_;
  return num_bytes_available_ - bytes_left_to_pull;
}

bool PullManager::OverQuota() { return RemainingQuota() < 0L; }

void PullManager::UpdatePullsBasedOnAvailableMemory(int64_t num_bytes_available) {
  if (num_bytes_available_ != num_bytes_available) {
    RAY_LOG(DEBUG) << "Updating pulls based on available memory: " << num_bytes_available;
  }
  num_bytes_available_ = num_bytes_available;
  // Assume that initially we have enough capacity for all
  // pulls. This will get set to true if there is at least one
  // bundle request that we cannot activate due to lack of
  // space.

  std::vector<ObjectID> objects_to_pull;
  std::unordered_set<ObjectID> object_ids_to_cancel;
  // If there are any get requests (highest priority), try to activate them. Since we
  // prioritize get requests over task and wait requests, these requests will be
  // canceled as necessary to make space. We may exit this block over capacity
  // if we run out of requests to cancel, but this will be remedied later
  // by canceling get and wait requests.
  bool get_requests_remaining = !get_request_bundles_.empty();
  while (get_requests_remaining) {
    int64_t margin_required =
        NextRequestBundleSize(get_request_bundles_, highest_get_req_id_being_pulled_);
    DeactivateUntilMarginAvailable("task args request", task_argument_bundles_,
                                   /*retain_min=*/0, /*quota_margin=*/margin_required,
                                   &highest_task_req_id_being_pulled_,
                                   &object_ids_to_cancel);
    DeactivateUntilMarginAvailable("wait request", wait_request_bundles_,
                                   /*retain_min=*/0, /*quota_margin=*/margin_required,
                                   &highest_wait_req_id_being_pulled_,
                                   &object_ids_to_cancel);

    // Activate the next get request unconditionally.
    get_requests_remaining = ActivateNextPullBundleRequest(
        get_request_bundles_, &highest_get_req_id_being_pulled_,
        /*respect_quota=*/false, &objects_to_pull);
  }

  // Do the same but for wait requests (medium priority).
  bool wait_requests_remaining = !wait_request_bundles_.empty();
  while (wait_requests_remaining) {
    int64_t margin_required =
        NextRequestBundleSize(wait_request_bundles_, highest_wait_req_id_being_pulled_);
    DeactivateUntilMarginAvailable("task args request", task_argument_bundles_,
                                   /*retain_min=*/0, /*quota_margin=*/margin_required,
                                   &highest_task_req_id_being_pulled_,
                                   &object_ids_to_cancel);

    // Activate the next wait request if we have space.
    wait_requests_remaining = ActivateNextPullBundleRequest(
        wait_request_bundles_, &highest_wait_req_id_being_pulled_,
        /*respect_quota=*/true, &objects_to_pull);
  }

  // Do the same but for task arg requests (lowest priority).
  // allowed for task arg requests.
  while (ActivateNextPullBundleRequest(task_argument_bundles_,
                                       &highest_task_req_id_being_pulled_,
                                       /*respect_quota=*/true, &objects_to_pull)) {
  }

  // While we are over capacity, deactivate requests starting from the back of the queues.
  DeactivateUntilMarginAvailable(
      "task args request", task_argument_bundles_, /*retain_min=*/1, /*quota_margin=*/0L,
      &highest_task_req_id_being_pulled_, &object_ids_to_cancel);
  DeactivateUntilMarginAvailable("wait request", wait_request_bundles_, /*retain_min=*/1,
                                 /*quota_margin=*/0L, &highest_wait_req_id_being_pulled_,
                                 &object_ids_to_cancel);

  // Call the cancellation callbacks outside of the lock.
  for (const auto &obj_id : object_ids_to_cancel) {
    cancel_pull_request_(obj_id);
  }

  {
    absl::MutexLock lock(&active_objects_mu_);
    for (const auto &obj_id : objects_to_pull) {
      if (object_ids_to_cancel.count(obj_id) == 0) {
        TryToMakeObjectLocal(obj_id);
      }
    }
  }
}

std::vector<ObjectID> PullManager::CancelPull(uint64_t request_id) {
  RAY_LOG(DEBUG) << "Cancel pull request " << request_id;

  Queue *request_queue = nullptr;
  uint64_t *highest_req_id_being_pulled = nullptr;
  auto bundle_it = get_request_bundles_.find(request_id);
  if (bundle_it != get_request_bundles_.end()) {
    request_queue = &get_request_bundles_;
    highest_req_id_being_pulled = &highest_get_req_id_being_pulled_;
  } else {
    bundle_it = wait_request_bundles_.find(request_id);
    if (bundle_it != wait_request_bundles_.end()) {
      request_queue = &wait_request_bundles_;
      highest_req_id_being_pulled = &highest_wait_req_id_being_pulled_;
    } else {
      bundle_it = task_argument_bundles_.find(request_id);
      request_queue = &task_argument_bundles_;
      highest_req_id_being_pulled = &highest_task_req_id_being_pulled_;
      RAY_CHECK(bundle_it != task_argument_bundles_.end());
    }
  }

  // If the pull request was being actively pulled, deactivate it now.
  if (bundle_it->first <= *highest_req_id_being_pulled) {
    std::unordered_set<ObjectID> object_ids_to_cancel;
    DeactivatePullBundleRequest(*request_queue, bundle_it, highest_req_id_being_pulled,
                                &object_ids_to_cancel);
    for (const auto &obj_id : object_ids_to_cancel) {
      // Call the cancellation callback outside of the lock.
      cancel_pull_request_(obj_id);
    }
  }

  // Erase this pull request.
  std::vector<ObjectID> object_ids_to_cancel_subscription;
  for (const auto &ref : bundle_it->second.objects) {
    auto obj_id = ObjectRefToId(ref);
    auto it = object_pull_requests_.find(obj_id);
    if (it != object_pull_requests_.end()) {
      RAY_LOG(DEBUG) << "Removing an object pull request of id: " << obj_id;
      it->second.bundle_request_ids.erase(bundle_it->first);
      if (it->second.bundle_request_ids.empty()) {
        object_pull_requests_.erase(it);
        object_ids_to_cancel_subscription.push_back(obj_id);
      }
    }
  }
  request_queue->erase(bundle_it);

  // We need to update the pulls in case there is another request(s) after this
  // request that can now be activated. We do this after erasing the cancelled
  // request to avoid reactivating it again.
  UpdatePullsBasedOnAvailableMemory(num_bytes_available_);

  return object_ids_to_cancel_subscription;
}

void PullManager::OnLocationChange(const ObjectID &object_id,
                                   const std::unordered_set<NodeID> &client_ids,
                                   const std::string &spilled_url,
                                   const NodeID &spilled_node_id, bool pending_creation,
                                   size_t object_size) {
  // Exit if the Pull request has already been fulfilled or canceled.
  auto it = object_pull_requests_.find(object_id);
  if (it == object_pull_requests_.end()) {
    return;
  }
  // Reset the list of clients that are now expected to have the object.
  // NOTE(swang): Since we are overwriting the previous list of clients,
  // we may end up sending a duplicate request to the same client as
  // before.
  it->second.client_locations = std::vector<NodeID>(client_ids.begin(), client_ids.end());
  it->second.spilled_url = spilled_url;
  it->second.spilled_node_id = spilled_node_id;
  it->second.pending_object_creation = pending_creation;
  if (!it->second.object_size_set) {
    // TODO(swang): This assumes that the object size will be set correctly on
    // the first location update and that locations will soon appear after the
    // object size has been set. This can block later requests whose metadata
    // has already arrived. Instead, we should keep track of which pull
    // requests are still waiting for object metadata and queue requests in the
    // order that their metadata appears.
    // See https://github.com/ray-project/ray/issues/13689.
    it->second.object_size = object_size;
    it->second.object_size_set = true;
    for (auto &bundle_request_id : it->second.bundle_request_ids) {
      auto bundle_it = get_request_bundles_.find(bundle_request_id);
      if (bundle_it == get_request_bundles_.end()) {
        bundle_it = wait_request_bundles_.find(bundle_request_id);
        if (bundle_it == wait_request_bundles_.end()) {
          bundle_it = task_argument_bundles_.find(bundle_request_id);
          RAY_CHECK(bundle_it != task_argument_bundles_.end());
        }
      }
      bundle_it->second.RegisterObjectSize(object_size);
    }

    UpdatePullsBasedOnAvailableMemory(num_bytes_available_);
    RAY_LOG(DEBUG) << "Updated size of object " << object_id << " to " << object_size
                   << ", num bytes being pulled is now " << num_bytes_being_pulled_;
    if (it->second.object_size == 0) {
      RAY_LOG(WARNING) << "Size of object " << object_id
                       << " stored in object store is zero. This may be a bug since "
                          "objects in the object store should be large, and can result "
                          "in too many objects being fetched to this node";
    }
  }
  RAY_LOG(DEBUG) << object_id << " OnLocationChange " << spilled_url << " num clients "
                 << client_ids.size();

  {
    absl::MutexLock lock(&active_objects_mu_);
    TryToMakeObjectLocal(object_id);
  }
}

void PullManager::TryToMakeObjectLocal(const ObjectID &object_id) {
  // The object is already local; abort.
  if (object_is_local_(object_id)) {
    return;
  }

  // The object is no longer needed; abort.
  if (active_object_pull_requests_.count(object_id) == 0) {
    return;
  }

  // The object waiting for local pull retry; abort.
  auto it = object_pull_requests_.find(object_id);
  RAY_CHECK(it != object_pull_requests_.end());
  auto &request = it->second;
  if (request.next_pull_time > get_time_seconds_()) {
    return;
  }

  // Try to pull the object from a remote node. If the object is spilled on the local
  // disk of the remote node, it will be restored by PushManager prior to pushing.
  bool did_pull = PullFromRandomLocation(object_id);
  if (did_pull) {
    UpdateRetryTimer(request, object_id);
    return;
  }

  // check if we can restore the object directly in the current raylet.
  // first check local spilled objects
  std::string direct_restore_url = get_locally_spilled_object_url_(object_id);
  if (direct_restore_url.empty()) {
    if (!request.spilled_url.empty() && request.spilled_node_id.IsNil()) {
      direct_restore_url = request.spilled_url;
    }
  }
  if (!direct_restore_url.empty()) {
    // Select an url from the object directory update
    UpdateRetryTimer(request, object_id);
    restore_spilled_object_(object_id, direct_restore_url,
                            [object_id](const ray::Status &status) {
                              if (!status.ok()) {
                                RAY_LOG(ERROR) << "Object restore for " << object_id
                                               << " failed, will retry later: " << status;
                              }
                            });
    return;
  }

  if (request.expiration_time_seconds == 0) {
    RAY_LOG(WARNING) << "Object neither in memory nor external storage "
                     << object_id.Hex();
    request.expiration_time_seconds =
        get_time_seconds_() +
        RayConfig::instance().fetch_fail_timeout_milliseconds() / 1e3;
  } else if (request.pending_object_creation) {
    // Object is pending creation, wait for the task that creates the object to
    // finish.
    RAY_LOG(INFO) << "Object pending creation " << object_id.Hex();
    request.expiration_time_seconds =
        get_time_seconds_() +
        RayConfig::instance().fetch_fail_timeout_milliseconds() / 1e3;
  } else if (get_time_seconds_() > request.expiration_time_seconds) {
    // Object has no locations and is not being reconstructed by its owner.
    fail_pull_request_(object_id);
    request.expiration_time_seconds = 0;
  }
}

bool PullManager::PullFromRandomLocation(const ObjectID &object_id) {
  auto it = object_pull_requests_.find(object_id);
  if (it == object_pull_requests_.end()) {
    return false;
  }

  auto &node_vector = it->second.client_locations;
  auto &spilled_node_id = it->second.spilled_node_id;

  if (node_vector.empty()) {
    // Pull from remote node, it will be restored prior to push.
    if (!spilled_node_id.IsNil() && spilled_node_id != self_node_id_) {
      send_pull_request_(object_id, spilled_node_id);
      return true;
    }
    // The timer should never fire if there are no expected client locations.
    return false;
  }

  RAY_CHECK(!object_is_local_(object_id));
  // Make sure that there is at least one client which is not the local client.
  // TODO(rkn): It may actually be possible for this check to fail.
  if (node_vector.size() == 1 && node_vector[0] == self_node_id_) {
    RAY_LOG(WARNING) << "The object manager with ID " << self_node_id_
                     << " is trying to pull object " << object_id
                     << " but the object table suggests that this object manager "
                     << "already has the object. The object may have been evicted. It is "
                     << "most likely due to memory pressure, object pull has been "
                     << "requested before object location is updated.";
    return false;
  }

  // Choose a random client to pull the object from.
  // Generate a random index.
  std::uniform_int_distribution<int> distribution(0, node_vector.size() - 1);
  int node_index = distribution(gen_);
  NodeID node_id = node_vector[node_index];
  // If the object manager somehow ended up choosing itself, choose a different
  // object manager.
  if (node_id == self_node_id_) {
    std::swap(node_vector[node_index], node_vector[node_vector.size() - 1]);
    node_vector.pop_back();
    RAY_LOG(WARNING)
        << "The object manager with ID " << self_node_id_ << " is trying to pull object "
        << object_id << " but the object table suggests that this object manager "
        << "already has the object. It is most likely due to memory pressure, object "
        << "pull has been requested before object location is updated.";
    node_id = node_vector[node_index % node_vector.size()];
    RAY_CHECK(node_id != self_node_id_);
  }

  RAY_LOG(DEBUG) << "Sending pull request from " << self_node_id_ << " to " << node_id
                 << " of object " << object_id;
  send_pull_request_(object_id, node_id);
  return true;
}

void PullManager::ResetRetryTimer(const ObjectID &object_id) {
  auto it = object_pull_requests_.find(object_id);
  if (it != object_pull_requests_.end()) {
    it->second.next_pull_time = get_time_seconds_();
    it->second.num_retries = 0;
    it->second.expiration_time_seconds = 0;
  }
}

void PullManager::UpdateRetryTimer(ObjectPullRequest &request,
                                   const ObjectID &object_id) {
  const auto time = get_time_seconds_();
  auto retry_timeout_len = (pull_timeout_ms_ / 1000.) * (1UL << request.num_retries);
  request.next_pull_time = time + retry_timeout_len;
  if (retry_timeout_len > max_timeout_) {
    max_timeout_ = retry_timeout_len;
    max_timeout_object_id_ = object_id;
  }

  if (request.num_retries > 0) {
    // We've tried this object before.
    num_retries_total_++;
  }

  // Bound the retry time at 10 * 1024 seconds.
  request.num_retries = std::min(request.num_retries + 1, 10);
  // Also reset the fetch expiration timer, since we just tried this pull.
  request.expiration_time_seconds = 0;
}

void PullManager::Tick() {
  absl::MutexLock lock(&active_objects_mu_);
  for (auto &pair : active_object_pull_requests_) {
    const auto &object_id = pair.first;
    TryToMakeObjectLocal(object_id);
  }
}

void PullManager::PinNewObjectIfNeeded(const ObjectID &object_id) {
  absl::MutexLock lock(&active_objects_mu_);
  bool active = active_object_pull_requests_.count(object_id) > 0;
  if (active) {
    if (TryPinObject(object_id)) {
      RAY_LOG(DEBUG) << "Pinned newly created object " << object_id;
    } else {
      RAY_LOG(DEBUG) << "Failed to pin newly created object " << object_id;
    }
  }
}

bool PullManager::TryPinObject(const ObjectID &object_id) {
  if (pinned_objects_.count(object_id) == 0) {
    auto ref = pin_object_(object_id);
    if (ref != nullptr) {
      pinned_objects_size_ += ref->GetSize();
      pinned_objects_[object_id] = std::move(ref);
      return false;
    }
  }
  return true;
}

void PullManager::UnpinObject(const ObjectID &object_id) {
  auto it = pinned_objects_.find(object_id);
  if (it != pinned_objects_.end()) {
    pinned_objects_size_ -= it->second->GetSize();
    pinned_objects_.erase(it);
  }
  if (pinned_objects_.empty()) {
    RAY_CHECK(pinned_objects_size_ == 0);
  }
}

int PullManager::NumActiveRequests() const { return object_pull_requests_.size(); }

bool PullManager::IsObjectActive(const ObjectID &object_id) const {
  absl::MutexLock lock(&active_objects_mu_);
  return active_object_pull_requests_.count(object_id) == 1;
}

bool PullManager::PullRequestActiveOrWaitingForMetadata(uint64_t request_id) const {
  const uint64_t *highest_req_id_being_pulled = nullptr;
  auto bundle_it = get_request_bundles_.find(request_id);
  if (bundle_it != get_request_bundles_.end()) {
    highest_req_id_being_pulled = &highest_get_req_id_being_pulled_;
  } else {
    bundle_it = wait_request_bundles_.find(request_id);
    if (bundle_it != wait_request_bundles_.end()) {
      highest_req_id_being_pulled = &highest_wait_req_id_being_pulled_;
    } else {
      bundle_it = task_argument_bundles_.find(request_id);
      RAY_CHECK(bundle_it != task_argument_bundles_.end());
      highest_req_id_being_pulled = &highest_task_req_id_being_pulled_;
    }
  }

  if (request_id <= *highest_req_id_being_pulled) {
    // This request is in the prefix of the queue that is being pulled.
    return true;
  }
  return bundle_it->second.num_object_sizes_missing > 0;
}

bool PullManager::HasPullsQueued() const {
  absl::MutexLock lock(&active_objects_mu_);
  return active_object_pull_requests_.size() != object_pull_requests_.size();
}

std::string PullManager::BundleInfo(const Queue &bundles,
                                    uint64_t highest_id_being_pulled) const {
  auto it = bundles.begin();
  if (it == bundles.end()) {
    return "N/A";
  }
  auto bundle = it->second;
  std::stringstream result;
  result << bundle.num_bytes_needed << " bytes, " << bundle.objects.size() << " objects";
  if (highest_id_being_pulled) {
    result << " (active)";
  } else {
    if (bundle.num_object_sizes_missing > 0) {
      result << " (inactive, waiting for object sizes)";
    } else {
      result << " (inactive, waiting for capacity)";
    }
  }
  return result.str();
}

int64_t PullManager::NextRequestBundleSize(const Queue &bundles,
                                           uint64_t highest_id_being_pulled) const {
  // Get the next pull request in the queue.
  const auto last_request_it = bundles.find(highest_id_being_pulled);
  auto next_request_it = last_request_it;
  if (next_request_it == bundles.end()) {
    // No requests are active. Get the first request in the queue.
    next_request_it = bundles.begin();
  } else {
    next_request_it++;
  }

  if (next_request_it == bundles.end()) {
    // No requests in the queue.
    return 0L;
  }

  if (next_request_it->second.num_object_sizes_missing > 0) {
    // There is at least one object size missing. We should not activate the
    // bundle, since it may put us over the available capacity.
    return 0L;
  }

  absl::MutexLock lock(&active_objects_mu_);

  // Calculate the bytes we need.
  int64_t bytes_needed_calculated = 0;
  for (const auto &ref : next_request_it->second.objects) {
    auto obj_id = ObjectRefToId(ref);
    bool needs_pull = active_object_pull_requests_.count(obj_id) == 0;
    if (needs_pull) {
      // This is the first bundle request in the queue to require this object.
      // Add the size to the number of bytes being pulled.
      auto it = object_pull_requests_.find(obj_id);
      RAY_CHECK(it != object_pull_requests_.end());
      bytes_needed_calculated += it->second.object_size;
    }
  }

  return bytes_needed_calculated;
}

void PullManager::RecordMetrics() const {
  absl::MutexLock lock(&active_objects_mu_);
  ray::stats::STATS_pull_manager_usage_bytes.Record(num_bytes_available_, "Available");
  ray::stats::STATS_pull_manager_usage_bytes.Record(num_bytes_being_pulled_,
                                                    "BeingPulled");
  ray::stats::STATS_pull_manager_usage_bytes.Record(pinned_objects_size_, "Pinned");
  ray::stats::STATS_pull_manager_requested_bundles.Record(get_request_bundles_.size(),
                                                          "Get");
  ray::stats::STATS_pull_manager_requested_bundles.Record(wait_request_bundles_.size(),
                                                          "Wait");
  ray::stats::STATS_pull_manager_requested_bundles.Record(task_argument_bundles_.size(),
                                                          "TaskArgs");
  ray::stats::STATS_pull_manager_requests.Record(object_pull_requests_.size(), "Queued");
  ray::stats::STATS_pull_manager_requests.Record(active_object_pull_requests_.size(),
                                                 "Active");
  ray::stats::STATS_pull_manager_requests.Record(pinned_objects_.size(), "Pinned");
  ray::stats::STATS_pull_manager_active_bundles.Record(num_active_bundles_);
  ray::stats::STATS_pull_manager_retries_total.Record(num_retries_total_);
}

std::string PullManager::DebugString() const {
  absl::MutexLock lock(&active_objects_mu_);
  std::stringstream result;
  result << "PullManager:";
  result << "\n- num bytes available for pulled objects: " << num_bytes_available_;
  result << "\n- num bytes being pulled (all): " << num_bytes_being_pulled_;
  result << "\n- num bytes being pulled / pinned: " << pinned_objects_size_;
  result << "\n- num get request bundles: " << get_request_bundles_.size();
  result << "\n- num wait request bundles: " << wait_request_bundles_.size();
  result << "\n- num task request bundles: " << task_argument_bundles_.size();
  result << "\n- first get request bundle: "
         << BundleInfo(get_request_bundles_, highest_get_req_id_being_pulled_);
  result << "\n- first wait request bundle: "
         << BundleInfo(wait_request_bundles_, highest_wait_req_id_being_pulled_);
  result << "\n- first task request bundle: "
         << BundleInfo(task_argument_bundles_, highest_task_req_id_being_pulled_);
  result << "\n- num objects queued: " << object_pull_requests_.size();
  result << "\n- num objects actively pulled (all): "
         << active_object_pull_requests_.size();
  result << "\n- num objects actively pulled / pinned: " << pinned_objects_.size();
  result << "\n- num bundles being pulled: " << num_active_bundles_;
  result << "\n- num pull retries: " << num_retries_total_;
  result << "\n- max timeout seconds: " << max_timeout_;
  auto it = object_pull_requests_.find(max_timeout_object_id_);
  if (it != object_pull_requests_.end()) {
    result << "\n- max timeout object id: " << max_timeout_object_id_;
    result << "\n- max timeout request location size: "
           << it->second.client_locations.size();
    result << "\n- max timeout request spilled url: " << it->second.spilled_url;
    result << "\n- max timeout request object size set: " << it->second.object_size_set;
    result << "\n- max timeout request object size: " << it->second.object_size;
  } else {
    result << "\n- max timeout request is already processed. No entry.";
  }
  // Guard this more expensive debug message under event stats.
  if (RayConfig::instance().event_stats()) {
    for (const auto &entry : active_object_pull_requests_) {
      auto obj_id = entry.first;
      if (!pinned_objects_.contains(obj_id)) {
        result << "\n- example obj id pending pull: " << obj_id.Hex();
        break;
      }
    }
  }
  return result.str();
}

}  // namespace ray
