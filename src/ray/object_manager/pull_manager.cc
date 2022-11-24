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
#include "ray/util/container_util.h"

namespace ray {

PullManager::PullManager(
    NodeID &self_node_id,
    const std::function<bool(const ObjectID &)> object_is_local,
    const std::function<void(const ObjectID &, const NodeID &)> send_pull_request,
    const std::function<void(const ObjectID &)> cancel_pull_request,
    const std::function<void(const ObjectID &, rpc::ErrorType)> fail_pull_request,
    const RestoreSpilledObjectCallback restore_spilled_object,
    const std::function<double()> get_time_seconds,
    int pull_timeout_ms,
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
                           const std::string &task_name,
                           std::vector<rpc::ObjectReference> *objects_to_locate) {
  // To avoid edge cases dealing with duplicated object ids in the bundle,
  // canonicalize the set up-front by dropping all duplicates.
  absl::flat_hash_set<ObjectID> seen;
  std::vector<rpc::ObjectReference> deduplicated;
  for (const auto &ref : object_ref_bundle) {
    const auto &id = ObjectRefToId(ref);
    if (seen.count(id) == 0) {
      seen.insert(id);
      deduplicated.emplace_back(ref);
    }
  }

  BundlePullRequest bundle_pull_request(ObjectRefsToIds(deduplicated), task_name);
  const uint64_t req_id = next_req_id_++;
  RAY_LOG(DEBUG) << "Start pull request " << req_id
                 << ". Bundle size: " << bundle_pull_request.objects.size();

  for (const auto &ref : deduplicated) {
    const auto obj_id = ObjectRefToId(ref);
    auto it = object_pull_requests_.find(obj_id);
    if (it == object_pull_requests_.end()) {
      RAY_LOG(DEBUG) << "Pull of object " << obj_id;
      // We don't have a pull for this object yet. Ask the caller to
      // send us notifications about the object's location.
      objects_to_locate->push_back(ref);
      // The first pull request doesn't need to be special case. Instead we can just let
      // the retry timer fire immediately.
      it = object_pull_requests_.emplace(obj_id, ObjectPullRequest(get_time_seconds_()))
               .first;
    } else {
      if (it->second.IsPullable()) {
        bundle_pull_request.MarkObjectAsPullable(obj_id);
      }
    }
    it->second.bundle_request_ids.insert(req_id);
  }

  if (prio == BundlePriority::GET_REQUEST) {
    get_request_bundles_.AddBundlePullRequest(req_id, std::move(bundle_pull_request));
  } else if (prio == BundlePriority::WAIT_REQUEST) {
    wait_request_bundles_.AddBundlePullRequest(req_id, std::move(bundle_pull_request));
  } else {
    RAY_CHECK(prio == BundlePriority::TASK_ARGS);
    task_argument_bundles_.AddBundlePullRequest(req_id, std::move(bundle_pull_request));
  }

  // We have a new request. Activate the new request, if the
  // current available memory allows it.
  UpdatePullsBasedOnAvailableMemory(num_bytes_available_);

  return req_id;
}

bool PullManager::ActivateNextBundlePullRequest(BundlePullRequestQueue &bundles,
                                                bool respect_quota,
                                                std::vector<ObjectID> *objects_to_pull) {
  if (bundles.inactive_requests.empty()) {
    // No inactive requests in the queue.
    return false;
  }

  // Get the next pull request in the queue.
  const auto next_request_id = *(bundles.inactive_requests.cbegin());
  const auto &next_request = map_find_or_die(bundles.requests, next_request_id);
  RAY_CHECK(next_request.IsPullable());

  // Activate the pull bundle request if possible.
  {
    absl::MutexLock lock(&active_objects_mu_);

    // First calculate the bytes we need.
    int64_t bytes_to_pull = 0;
    for (const auto &obj_id : next_request.objects) {
      const bool needs_pull = active_object_pull_requests_.count(obj_id) == 0;
      if (needs_pull) {
        // This is the first bundle request in the queue to require this object.
        // Add the size to the number of bytes being pulled.
        // TODO(ekl) this overestimates bytes needed if it's already available
        // locally.
        bytes_to_pull += map_find_or_die(object_pull_requests_, obj_id).object_size;
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

    RAY_LOG(DEBUG) << "Activating request " << next_request_id
                   << " num bytes being pulled: " << num_bytes_being_pulled_
                   << " num bytes available: " << num_bytes_available_;
    num_bytes_being_pulled_ += bytes_to_pull;
    for (const auto &obj_id : next_request.objects) {
      const bool needs_pull = active_object_pull_requests_.count(obj_id) == 0;
      active_object_pull_requests_[obj_id].insert(next_request_id);
      if (needs_pull) {
        RAY_LOG(DEBUG) << "Activating pull for object " << obj_id;
        auto &request = map_find_or_die(object_pull_requests_, obj_id);
        request.activate_time_ms = absl::GetCurrentTimeNanos() / 1e3;

        TryPinObject(obj_id);
        objects_to_pull->push_back(obj_id);
        ResetRetryTimer(obj_id);
      }
    }
  }

  bundles.ActivateBundlePullRequest(next_request_id);

  num_active_bundles_ += 1;
  return true;
}

void PullManager::DeactivateBundlePullRequest(
    BundlePullRequestQueue &bundles,
    uint64_t request_id,
    std::unordered_set<ObjectID> *objects_to_cancel) {
  const auto &request = map_find_or_die(bundles.requests, request_id);
  for (const auto &obj_id : request.objects) {
    absl::MutexLock lock(&active_objects_mu_);
    auto it = active_object_pull_requests_.find(obj_id);
    if (it == active_object_pull_requests_.end() || !it->second.erase(request_id)) {
      // The object is already deactivated, no action is required.
      continue;
    }
    if (it->second.empty()) {
      RAY_LOG(DEBUG) << "Deactivating pull for object " << obj_id;
      num_bytes_being_pulled_ -=
          map_find_or_die(object_pull_requests_, obj_id).object_size;
      active_object_pull_requests_.erase(obj_id);
      UnpinObject(obj_id);
      objects_to_cancel->insert(obj_id);
    }
  }

  bundles.DeactivateBundlePullRequest(request_id);
  num_active_bundles_ -= 1;
}

void PullManager::DeactivateUntilMarginAvailable(
    const std::string &debug_name,
    BundlePullRequestQueue &bundles,
    int retain_min,
    int64_t quota_margin,
    std::unordered_set<ObjectID> *object_ids_to_cancel) {
  while (RemainingQuota() < quota_margin && !bundles.active_requests.empty()) {
    if (num_active_bundles_ <= retain_min) {
      return;
    }
    const uint64_t request_id = *(bundles.active_requests.rbegin());
    RAY_LOG(DEBUG) << "Deactivating " << debug_name << " " << request_id
                   << " num bytes being pulled: " << num_bytes_being_pulled_
                   << " num bytes available: " << num_bytes_available_;
    DeactivateBundlePullRequest(bundles, request_id, object_ids_to_cancel);
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

  std::vector<ObjectID> objects_to_pull;
  std::unordered_set<ObjectID> object_ids_to_cancel;
  // If there are any get requests (highest priority), try to activate them. Since we
  // prioritize get requests over task and wait requests, these requests will be
  // canceled as necessary to make space. We may exit this block over capacity
  // if we run out of requests to cancel, but this will be remedied later
  // by canceling task args and wait requests.
  bool get_requests_remaining = !get_request_bundles_.inactive_requests.empty();
  while (get_requests_remaining) {
    const int64_t margin_required = NextRequestBundleSize(get_request_bundles_);
    DeactivateUntilMarginAvailable("task args request",
                                   task_argument_bundles_,
                                   /*retain_min=*/0,
                                   /*quota_margin=*/margin_required,
                                   &object_ids_to_cancel);
    DeactivateUntilMarginAvailable("wait request",
                                   wait_request_bundles_,
                                   /*retain_min=*/0,
                                   /*quota_margin=*/margin_required,
                                   &object_ids_to_cancel);

    // Activate the next get request unconditionally.
    get_requests_remaining = ActivateNextBundlePullRequest(get_request_bundles_,
                                                           /*respect_quota=*/false,
                                                           &objects_to_pull);
  }

  // Do the same but for wait requests (medium priority).
  bool wait_requests_remaining = !wait_request_bundles_.inactive_requests.empty();
  while (wait_requests_remaining) {
    const int64_t margin_required = NextRequestBundleSize(wait_request_bundles_);
    DeactivateUntilMarginAvailable("task args request",
                                   task_argument_bundles_,
                                   /*retain_min=*/0,
                                   /*quota_margin=*/margin_required,
                                   &object_ids_to_cancel);

    // Activate the next wait request if we have space.
    wait_requests_remaining = ActivateNextBundlePullRequest(wait_request_bundles_,
                                                            /*respect_quota=*/true,
                                                            &objects_to_pull);
  }

  // Do the same but for task arg requests (lowest priority).
  // allowed for task arg requests.
  while (ActivateNextBundlePullRequest(task_argument_bundles_,
                                       /*respect_quota=*/true,
                                       &objects_to_pull)) {
  }

  // While we are over capacity, deactivate requests starting from the back of the queues.
  DeactivateUntilMarginAvailable("task args request",
                                 task_argument_bundles_,
                                 /*retain_min=*/1,
                                 /*quota_margin=*/0L,
                                 &object_ids_to_cancel);
  DeactivateUntilMarginAvailable("wait request",
                                 wait_request_bundles_,
                                 /*retain_min=*/1,
                                 /*quota_margin=*/0L,
                                 &object_ids_to_cancel);

  // Call the cancellation callbacks outside of the lock.
  for (const auto &obj_id : object_ids_to_cancel) {
    RAY_LOG(DEBUG) << "Not enough memory to create requested object " << obj_id
                   << ", aborting.";
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

  BundlePullRequestQueue &bundles = GetBundlePullRequestQueue(request_id);
  auto bundle_it = bundles.requests.find(request_id);
  RAY_CHECK(bundle_it != bundles.requests.end());

  // If the pull request was being actively pulled, deactivate it now.
  if (bundles.active_requests.count(request_id) > 0) {
    std::unordered_set<ObjectID> object_ids_to_cancel;
    DeactivateBundlePullRequest(bundles, request_id, &object_ids_to_cancel);
    for (const auto &obj_id : object_ids_to_cancel) {
      // Call the cancellation callback outside of the lock.
      RAY_LOG(DEBUG) << "Pull cancellation requested for object " << obj_id
                     << ", aborting creation.";
      cancel_pull_request_(obj_id);
    }
  }

  // Erase this pull request.
  std::vector<ObjectID> object_ids_to_cancel_subscription;
  for (const auto &obj_id : bundle_it->second.objects) {
    auto it = object_pull_requests_.find(obj_id);
    if (it != object_pull_requests_.end()) {
      RAY_LOG(DEBUG) << "Removing an object pull request of id: " << obj_id;
      it->second.bundle_request_ids.erase(bundle_it->first);
      if (it->second.bundle_request_ids.empty()) {
        ray::stats::STATS_pull_manager_object_request_time_ms.Record(
            absl::GetCurrentTimeNanos() / 1e3 - it->second.request_start_time_ms,
            "StartToCancel");
        object_pull_requests_.erase(it);
        object_ids_to_cancel_subscription.push_back(obj_id);
      }
    }
  }
  bundles.RemoveBundlePullRequest(request_id);

  // We need to update the pulls in case there is another request(s) after this
  // request that can now be activated. We do this after erasing the cancelled
  // request to avoid reactivating it again.
  UpdatePullsBasedOnAvailableMemory(num_bytes_available_);

  return object_ids_to_cancel_subscription;
}

void PullManager::OnLocationChange(const ObjectID &object_id,
                                   const std::unordered_set<NodeID> &client_ids,
                                   const std::string &spilled_url,
                                   const NodeID &spilled_node_id,
                                   bool pending_creation,
                                   size_t object_size) {
  // Exit if the Pull request has already been fulfilled or canceled.
  auto it = object_pull_requests_.find(object_id);
  if (it == object_pull_requests_.end()) {
    return;
  }

  const bool was_pullable_before = it->second.IsPullable();
  // Reset the list of clients that are now expected to have the object.
  // NOTE(swang): Since we are overwriting the previous list of clients,
  // we may end up sending a duplicate request to the same client as
  // before.
  it->second.client_locations.clear();
  for (const auto &client_id : client_ids) {
    if (client_id != self_node_id_) {
      // We can't pull from ourselves, so filter these locations out.
      // NOTE(swang): This means that we may try to pull an object even though
      // the directory says that we already have the object local in plasma.
      // This can happen due to a race condition between asynchronous updates
      // or a bug in the object directory.
      it->second.client_locations.push_back(client_id);
    }
  }
  it->second.spilled_url = spilled_url;
  it->second.spilled_node_id = spilled_node_id;
  it->second.pending_object_creation = pending_creation;
  if (!it->second.object_size_set) {
    it->second.object_size = object_size;
    it->second.object_size_set = true;

    if (it->second.object_size == 0) {
      RAY_LOG(WARNING) << "Size of object " << object_id
                       << " stored in object store is zero. This may be a bug since "
                          "objects in the object store should be large, and can result "
                          "in too many objects being fetched to this node";
    }
  }
  const bool is_pullable_after = it->second.IsPullable();

  if (was_pullable_before != is_pullable_after) {
    for (auto &bundle_request_id : it->second.bundle_request_ids) {
      BundlePullRequestQueue &bundles = GetBundlePullRequestQueue(bundle_request_id);
      auto bundle_it = bundles.requests.find(bundle_request_id);
      RAY_CHECK(bundle_it != bundles.requests.end());
      if (is_pullable_after) {
        bundle_it->second.MarkObjectAsPullable(object_id);
        if (bundle_it->second.IsPullable()) {
          bundles.MarkBundleAsPullable(bundle_request_id);
        }
      } else {
        bundle_it->second.MarkObjectAsUnpullable(object_id);
        RAY_CHECK(!bundle_it->second.IsPullable());
        if (bundles.active_requests.count(bundle_request_id) > 0) {
          // It's active now so we need to deactivate it
          // to free memory for other requests.
          std::unordered_set<ObjectID> objects_to_cancel;
          DeactivateBundlePullRequest(bundles, bundle_request_id, &objects_to_cancel);
          for (const auto &obj_id : objects_to_cancel) {
            cancel_pull_request_(obj_id);
          }
        }
        bundles.MarkBundleAsUnpullable(bundle_request_id);
      }
    }

    UpdatePullsBasedOnAvailableMemory(num_bytes_available_);
    RAY_LOG(DEBUG) << "Updated location of " << object_id
                   << ", num bytes being pulled is now " << num_bytes_being_pulled_;
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
  auto &request = map_find_or_die(object_pull_requests_, object_id);
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
    restore_spilled_object_(object_id,
                            request.object_size,
                            direct_restore_url,
                            [object_id](const ray::Status &status) {
                              if (!status.ok()) {
                                RAY_LOG(ERROR) << "Object restore for " << object_id
                                               << " failed, will retry later: " << status;
                              }
                            });
    return;
  }

  RAY_CHECK(!request.pending_object_creation);
  if (request.expiration_time_seconds == 0) {
    RAY_LOG(WARNING) << "Object neither in memory nor external storage "
                     << object_id.Hex();
    request.expiration_time_seconds =
        get_time_seconds_() +
        RayConfig::instance().fetch_fail_timeout_milliseconds() / 1e3;
  } else if (get_time_seconds_() > request.expiration_time_seconds) {
    // Object has no locations and is not being reconstructed by its owner.
    fail_pull_request_(object_id, rpc::ErrorType::OBJECT_FETCH_TIMED_OUT);
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
      RAY_LOG(DEBUG) << "Sending pull request from " << self_node_id_
                     << " to spilled location at " << spilled_node_id << " of object "
                     << object_id;
      send_pull_request_(object_id, spilled_node_id);
      return true;
    }
    // The timer should never fire if there are no expected client locations.
    return false;
  }

  RAY_CHECK(!object_is_local_(object_id));

  // Choose a random client to pull the object from.
  // Generate a random index.
  std::uniform_int_distribution<int> distribution(0, node_vector.size() - 1);
  int node_index = distribution(gen_);
  NodeID node_id = node_vector[node_index];
  RAY_CHECK(node_id != self_node_id_);
  RAY_LOG(DEBUG) << "Sending pull request from " << self_node_id_
                 << " to in-memory location at " << node_id << " of object " << object_id;
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

  num_tries_total_++;
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
      num_succeeded_pins_total_++;
      pinned_objects_size_ += ref->GetSize();
      pinned_objects_[object_id] = std::move(ref);

      auto it = object_pull_requests_.find(object_id);
      RAY_CHECK(it != object_pull_requests_.end());
      ray::stats::STATS_pull_manager_object_request_time_ms.Record(
          absl::GetCurrentTimeNanos() / 1e3 - it->second.request_start_time_ms,
          "StartToPin");
      if (it->second.activate_time_ms > 0) {
        ray::stats::STATS_pull_manager_object_request_time_ms.Record(
            absl::GetCurrentTimeNanos() / 1e3 - it->second.activate_time_ms,
            "MemoryAvailableToPin");
      }
    } else {
      num_failed_pins_total_++;
    }
  }
  return pinned_objects_.count(object_id) > 0;
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

int PullManager::NumObjectPullRequests() const { return object_pull_requests_.size(); }

bool PullManager::IsObjectActive(const ObjectID &object_id) const {
  absl::MutexLock lock(&active_objects_mu_);
  return active_object_pull_requests_.count(object_id) == 1;
}

const PullManager::BundlePullRequestQueue &PullManager::GetBundlePullRequestQueue(
    uint64_t request_id) const {
  if (get_request_bundles_.requests.contains(request_id)) {
    return get_request_bundles_;
  } else if (wait_request_bundles_.requests.contains(request_id)) {
    return wait_request_bundles_;
  } else {
    RAY_CHECK(task_argument_bundles_.requests.contains(request_id));
    return task_argument_bundles_;
  }
}

PullManager::BundlePullRequestQueue &PullManager::GetBundlePullRequestQueue(
    uint64_t request_id) {
  return const_cast<BundlePullRequestQueue &>(
      const_cast<const PullManager *>(this)->GetBundlePullRequestQueue(request_id));
}

bool PullManager::PullRequestActiveOrWaitingForMetadata(uint64_t request_id) const {
  const BundlePullRequestQueue &bundles = GetBundlePullRequestQueue(request_id);

  // If a request isn't inactive then it must be
  // either active or unpullable (i.e. waiting for metadata).
  return bundles.inactive_requests.count(request_id) == 0;
}

bool PullManager::HasPullsQueued() const {
  absl::MutexLock lock(&active_objects_mu_);
  return active_object_pull_requests_.size() != object_pull_requests_.size();
}

std::string PullManager::BundleInfo(const BundlePullRequestQueue &bundles) const {
  auto it = bundles.requests.begin();
  if (it == bundles.requests.end()) {
    return "N/A";
  }
  const auto &bundle = it->second;
  std::stringstream result;
  result << bundle.objects.size() << " objects";
  if (!bundle.IsPullable()) {
    result << " (inactive, waiting for object sizes or locations)";
  } else {
    size_t num_bytes_needed = 0;
    for (const auto &obj_id : bundle.objects) {
      num_bytes_needed += map_find_or_die(object_pull_requests_, obj_id).object_size;
    }
    result << ", " << num_bytes_needed << " bytes";
    if (bundles.active_requests.count(it->first) > 0) {
      result << " (active)";
    } else {
      result << " (inactive, waiting for capacity)";
    }
  }

  return result.str();
}

int64_t PullManager::NextRequestBundleSize(const BundlePullRequestQueue &bundles) const {
  if (bundles.inactive_requests.empty()) {
    // No inactive requests in the queue.
    return 0L;
  }
  // Get the next pull request in the queue.
  uint64_t next_request_id = *(bundles.inactive_requests.cbegin());
  const auto &next_request = map_find_or_die(bundles.requests, next_request_id);
  RAY_CHECK(next_request.IsPullable());

  absl::MutexLock lock(&active_objects_mu_);

  // Calculate the bytes we need.
  int64_t bytes_needed_calculated = 0;
  for (const auto &obj_id : next_request.objects) {
    bool needs_pull = active_object_pull_requests_.count(obj_id) == 0;
    if (needs_pull) {
      // This is the first bundle request in the queue to require this object.
      // Add the size to the number of bytes being pulled.
      bytes_needed_calculated +=
          map_find_or_die(object_pull_requests_, obj_id).object_size;
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
  ray::stats::STATS_pull_manager_requested_bundles.Record(
      get_request_bundles_.requests.size(), "Get");
  ray::stats::STATS_pull_manager_requested_bundles.Record(
      wait_request_bundles_.requests.size(), "Wait");
  ray::stats::STATS_pull_manager_requested_bundles.Record(
      task_argument_bundles_.requests.size(), "TaskArgs");
  ray::stats::STATS_pull_manager_requested_bundles.Record(next_req_id_,
                                                          "CumulativeTotal");
  ray::stats::STATS_pull_manager_requests.Record(object_pull_requests_.size(), "Queued");
  ray::stats::STATS_pull_manager_requests.Record(active_object_pull_requests_.size(),
                                                 "Active");
  ray::stats::STATS_pull_manager_requests.Record(pinned_objects_.size(), "Pinned");
  ray::stats::STATS_pull_manager_active_bundles.Record(num_active_bundles_);
  ray::stats::STATS_pull_manager_retries_total.Record(num_retries_total_);
  ray::stats::STATS_pull_manager_retries_total.Record(num_tries_total_);

  ray::stats::STATS_pull_manager_num_object_pins.Record(num_succeeded_pins_total_,
                                                        "Success");
  ray::stats::STATS_pull_manager_num_object_pins.Record(num_failed_pins_total_,
                                                        "Failure");
}

std::string PullManager::DebugString() const {
  absl::MutexLock lock(&active_objects_mu_);
  std::stringstream result;
  result << "PullManager:";
  result << "\n- num bytes available for pulled objects: " << num_bytes_available_;
  result << "\n- num bytes being pulled (all): " << num_bytes_being_pulled_;
  result << "\n- num bytes being pulled / pinned: " << pinned_objects_size_;
  result << "\n- get request bundles: " << get_request_bundles_.DebugString();
  result << "\n- wait request bundles: " << wait_request_bundles_.DebugString();
  result << "\n- task request bundles: " << task_argument_bundles_.DebugString();
  result << "\n- first get request bundle: " << BundleInfo(get_request_bundles_);
  result << "\n- first wait request bundle: " << BundleInfo(wait_request_bundles_);
  result << "\n- first task request bundle: " << BundleInfo(task_argument_bundles_);
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
    result << "\n- max timeout object: " << it->second.DebugString();
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

void PullManager::SetOutOfDisk(const ObjectID &object_id) {
  bool is_actively_pulled = false;
  {
    absl::MutexLock lock(&active_objects_mu_);
    is_actively_pulled = active_object_pull_requests_.count(object_id) > 0;
  }
  if (is_actively_pulled) {
    RAY_LOG(DEBUG) << "Pull of object failed due to out of disk: " << object_id;
    fail_pull_request_(object_id, rpc::ErrorType::OUT_OF_DISK_ERROR);
  }
}

}  // namespace ray
