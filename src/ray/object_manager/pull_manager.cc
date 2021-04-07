#include "ray/object_manager/pull_manager.h"

#include "ray/common/common_protocol.h"

namespace ray {

PullManager::PullManager(
    NodeID &self_node_id, const std::function<bool(const ObjectID &)> object_is_local,
    const std::function<void(const ObjectID &, const NodeID &)> send_pull_request,
    const std::function<void(const ObjectID &)> cancel_pull_request,
    const RestoreSpilledObjectCallback restore_spilled_object,
    const std::function<double()> get_time, int pull_timeout_ms,
    size_t num_bytes_available, std::function<void()> object_store_full_callback)
    : self_node_id_(self_node_id),
      object_is_local_(object_is_local),
      send_pull_request_(send_pull_request),
      cancel_pull_request_(cancel_pull_request),
      restore_spilled_object_(restore_spilled_object),
      get_time_(get_time),
      pull_timeout_ms_(pull_timeout_ms),
      num_bytes_available_(num_bytes_available),
      object_store_full_callback_(object_store_full_callback),
      gen_(std::chrono::high_resolution_clock::now().time_since_epoch().count()) {}

uint64_t PullManager::Pull(const std::vector<rpc::ObjectReference> &object_ref_bundle,
                           std::vector<rpc::ObjectReference> *objects_to_locate) {
  auto bundle_it =
      pull_request_bundles_.emplace(next_req_id_++, std::move(object_ref_bundle)).first;
  RAY_LOG(DEBUG) << "Start pull request " << bundle_it->first
                 << ". Bundle size: " << bundle_it->second.objects.size();

  for (const auto &ref : object_ref_bundle) {
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
               .emplace(obj_id, ObjectPullRequest(/*next_pull_time=*/get_time_()))
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

bool PullManager::ActivateNextPullBundleRequest(
    const std::map<uint64_t, PullBundleRequest>::iterator &next_request_it,
    std::vector<ObjectID> *objects_to_pull) {
  // Activate the bundle.
  for (const auto &ref : next_request_it->second.objects) {
    absl::MutexLock lock(&active_objects_mu_);
    auto obj_id = ObjectRefToId(ref);
    bool start_pull = active_object_pull_requests_.count(obj_id) == 0;
    active_object_pull_requests_[obj_id].insert(next_request_it->first);
    if (start_pull) {
      RAY_LOG(DEBUG) << "Activating pull for object " << obj_id;
      // This is the first bundle request in the queue to require this object.
      // Add the size to the number of bytes being pulled.
      auto it = object_pull_requests_.find(obj_id);
      RAY_CHECK(it != object_pull_requests_.end());
      num_bytes_being_pulled_ += it->second.object_size;
      objects_to_pull->push_back(obj_id);

      ResetRetryTimer(obj_id);
    }
  }

  // Update the pointer to the last pull request that we are actively pulling.
  RAY_CHECK(next_request_it->first > highest_req_id_being_pulled_);
  highest_req_id_being_pulled_ = next_request_it->first;
  return true;
}

void PullManager::DeactivatePullBundleRequest(
    const std::map<uint64_t, PullBundleRequest>::iterator &request_it,
    std::unordered_set<ObjectID> *objects_to_cancel) {
  for (const auto &ref : request_it->second.objects) {
    absl::MutexLock lock(&active_objects_mu_);
    auto obj_id = ObjectRefToId(ref);
    if (!active_object_pull_requests_[obj_id].erase(request_it->first)) {
      // If a bundle contains multiple duplicated object ids, the active pull request
      // could've been already removed. Then do nothing.
      continue;
    }
    if (active_object_pull_requests_[obj_id].empty()) {
      RAY_LOG(DEBUG) << "Deactivating pull for object " << obj_id;
      auto it = object_pull_requests_.find(obj_id);
      RAY_CHECK(it != object_pull_requests_.end());
      num_bytes_being_pulled_ -= it->second.object_size;
      active_object_pull_requests_.erase(obj_id);
      objects_to_cancel->insert(obj_id);
    }
  }

  // If this was the last active request, update the pointer to its
  // predecessor, if one exists.
  if (highest_req_id_being_pulled_ == request_it->first) {
    if (request_it == pull_request_bundles_.begin()) {
      highest_req_id_being_pulled_ = 0;
    } else {
      highest_req_id_being_pulled_ = std::prev(request_it)->first;
    }
  }
}

void PullManager::UpdatePullsBasedOnAvailableMemory(size_t num_bytes_available) {
  if (num_bytes_available_ != num_bytes_available) {
    RAY_LOG(DEBUG) << "Updating pulls based on available memory: " << num_bytes_available;
  }
  num_bytes_available_ = num_bytes_available;

  // While there is available capacity, activate the next pull request.
  std::vector<ObjectID> objects_to_pull;
  while (num_bytes_being_pulled_ < num_bytes_available_) {
    // Get the next pull request in the queue.
    const auto last_request_it = pull_request_bundles_.find(highest_req_id_being_pulled_);
    auto next_request_it = last_request_it;
    if (next_request_it == pull_request_bundles_.end()) {
      // No requests are active. Get the first request in the queue.
      next_request_it = pull_request_bundles_.begin();
    } else {
      next_request_it++;
    }

    if (next_request_it == pull_request_bundles_.end()) {
      // No requests in the queue.
      break;
    }

    if (next_request_it->second.num_object_sizes_missing > 0) {
      // There is at least one object size missing. We should not activate the
      // bundle, since it may put us over the available capacity.
      break;
    }

    RAY_LOG(DEBUG) << "Activating request " << next_request_it->first
                   << " num bytes being pulled: " << num_bytes_being_pulled_
                   << " num bytes available: " << num_bytes_available_;
    // There is another pull bundle request that we could try, and there is
    // enough space. Activate the next pull bundle request in the queue.
    if (!ActivateNextPullBundleRequest(next_request_it, &objects_to_pull)) {
      // This pull bundle request could not be activated, due to lack of object
      // size information. Wait until we have object size information before
      // activating this pull bundle.
      break;
    }
  }

  std::unordered_set<ObjectID> object_ids_to_cancel;
  // While the total bytes requested is over the available capacity, deactivate
  // the last pull request, ordered by request ID.
  while (num_bytes_being_pulled_ > num_bytes_available_) {
    RAY_LOG(DEBUG) << "Deactivating request " << highest_req_id_being_pulled_
                   << " num bytes being pulled: " << num_bytes_being_pulled_
                   << " num bytes available: " << num_bytes_available_;
    const auto last_request_it = pull_request_bundles_.find(highest_req_id_being_pulled_);
    RAY_CHECK(last_request_it != pull_request_bundles_.end());
    DeactivatePullBundleRequest(last_request_it, &object_ids_to_cancel);
  }
  for (const auto &obj_id : object_ids_to_cancel) {
    // Call the cancellation callback outside of the lock.
    cancel_pull_request_(obj_id);
  }

  TriggerOutOfMemoryHandlingIfNeeded();

  {
    absl::MutexLock lock(&active_objects_mu_);
    for (const auto &obj_id : objects_to_pull) {
      if (object_ids_to_cancel.count(obj_id) == 0) {
        TryToMakeObjectLocal(obj_id);
      }
    }
  }
}

void PullManager::TriggerOutOfMemoryHandlingIfNeeded() {
  if (pull_request_bundles_.empty()) {
    // No requests queued.
    return;
  }

  const auto head = pull_request_bundles_.begin();
  if (highest_req_id_being_pulled_ >= head->first) {
    // At least one request is being actively pulled, so there is currently
    // enough space.
    return;
  }

  // No requests are being pulled. Check whether this is because we don't have
  // object size information yet.
  if (head->second.num_object_sizes_missing > 0) {
    // Wait for the size information before triggering OOM
    return;
  }

  // The first request in the queue is not being pulled due to lack of space.
  // Trigger out-of-memory handling to try to make room.
  // TODO(swang): This can hang if no room can be made. We should return an
  // error for requests whose total size is larger than the capacity of the
  // memory store.
  if (get_time_() - last_oom_reported_ms_ > 30000) {
    RAY_LOG(WARNING)
        << "There is not enough memory to pull objects needed by a queued task or "
           "a worker blocked in ray.get or ray.wait. "
        << "Need " << head->second.num_bytes_needed << " bytes, but only "
        << num_bytes_available_ << " bytes are available on this node. "
        << "This job may hang if no memory can be freed through garbage collection or "
           "object spilling. See "
           "https://docs.ray.io/en/master/memory-management.html for more information. "
           "Please file a GitHub issue if you see this message repeatedly.";
    last_oom_reported_ms_ = get_time_();
  }
  object_store_full_callback_();
}

std::vector<ObjectID> PullManager::CancelPull(uint64_t request_id) {
  RAY_LOG(DEBUG) << "Cancel pull request " << request_id;
  auto bundle_it = pull_request_bundles_.find(request_id);
  RAY_CHECK(bundle_it != pull_request_bundles_.end());

  // If the pull request was being actively pulled, deactivate it now.
  if (bundle_it->first <= highest_req_id_being_pulled_) {
    std::unordered_set<ObjectID> object_ids_to_cancel;
    DeactivatePullBundleRequest(bundle_it, &object_ids_to_cancel);
    for (const auto &obj_id : object_ids_to_cancel) {
      // Call the cancellation callback outside of the lock.
      cancel_pull_request_(obj_id);
    }
  }

  // Erase this pull request.
  std::vector<ObjectID> object_ids_to_cancel_subscription;
  for (const auto &ref : bundle_it->second.objects) {
    auto obj_id = ObjectRefToId(ref);
    RAY_LOG(DEBUG) << "Removing an object pull request of id: " << obj_id;
    auto it = object_pull_requests_.find(obj_id);
    if (it == object_pull_requests_.end()) {
      // If there are duplicated object ids in the bundle,
      // it is possible we already pull the object.
      continue;
    }
    RAY_CHECK(it->second.bundle_request_ids.erase(bundle_it->first));
    if (it->second.bundle_request_ids.empty()) {
      object_pull_requests_.erase(it);
      object_ids_to_cancel_subscription.push_back(obj_id);
    }
  }
  pull_request_bundles_.erase(bundle_it);

  // We need to update the pulls in case there is another request(s) after this
  // request that can now be activated. We do this after erasing the cancelled
  // request to avoid reactivating it again.
  UpdatePullsBasedOnAvailableMemory(num_bytes_available_);

  return object_ids_to_cancel_subscription;
}

void PullManager::OnLocationChange(const ObjectID &object_id,
                                   const std::unordered_set<NodeID> &client_ids,
                                   const std::string &spilled_url,
                                   const NodeID &spilled_node_id, size_t object_size) {
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
  if (!it->second.object_size_set) {
    it->second.object_size = object_size;
    it->second.object_size_set = true;
    for (auto &bundle_request_id : it->second.bundle_request_ids) {
      auto bundle_it = pull_request_bundles_.find(bundle_request_id);
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
  RAY_LOG(DEBUG) << "OnLocationChange " << spilled_url << " num clients "
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
  if (request.next_pull_time > get_time_()) {
    return;
  }

  // Try to pull the object from a remote node. If the object is spilled on the local
  // disk of the remote node, it will be restored by PushManager prior to pushing.
  bool did_pull = PullFromRandomLocation(object_id);
  if (did_pull) {
    UpdateRetryTimer(request);
    return;
  }

  // If we can restore directly from this raylet, then try to do so.
  bool can_restore_directly =
      !request.spilled_url.empty() &&
      (request.spilled_node_id.IsNil() || request.spilled_node_id == self_node_id_);
  if (can_restore_directly) {
    UpdateRetryTimer(request);
    restore_spilled_object_(object_id, request.spilled_url,
                            [object_id](const ray::Status &status) {
                              if (!status.ok()) {
                                RAY_LOG(ERROR) << "Object restore for " << object_id
                                               << " failed, will retry later: " << status;
                              }
                            });
    return;
  }

  // TODO(ekl) should we more directly mark the object as lost in this case?
  RAY_LOG(DEBUG) << "Object neither in memory nor external storage " << object_id.Hex();
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
    it->second.next_pull_time = get_time_();
    it->second.num_retries = 0;
  }
}

void PullManager::UpdateRetryTimer(ObjectPullRequest &request) {
  const auto time = get_time_();
  auto retry_timeout_len = (pull_timeout_ms_ / 1000.) * (1UL << request.num_retries);
  request.next_pull_time = time + retry_timeout_len;

  // Bound the retry time at 10 * 1024 seconds.
  request.num_retries = std::min(request.num_retries + 1, 10);
}

void PullManager::Tick() {
  absl::MutexLock lock(&active_objects_mu_);
  for (auto &pair : active_object_pull_requests_) {
    const auto &object_id = pair.first;
    TryToMakeObjectLocal(object_id);
  }
}

int PullManager::NumActiveRequests() const { return object_pull_requests_.size(); }

bool PullManager::IsObjectActive(const ObjectID &object_id) const {
  absl::MutexLock lock(&active_objects_mu_);
  return active_object_pull_requests_.count(object_id) == 1;
}

bool PullManager::PullRequestActiveOrWaitingForMetadata(uint64_t request_id) const {
  if (request_id <= highest_req_id_being_pulled_) {
    // This request is in the prefix of the queue that is being pulled.
    return true;
  }
  auto it = pull_request_bundles_.find(request_id);
  RAY_CHECK(it != pull_request_bundles_.end());
  return it->second.num_object_sizes_missing > 0;
}

std::string PullManager::DebugString() const {
  absl::MutexLock lock(&active_objects_mu_);
  std::stringstream result;
  result << "PullManager:";
  result << "\n- num bytes available for pulled objects: " << num_bytes_available_;
  result << "\n- num bytes being pulled: " << num_bytes_being_pulled_;
  result << "\n- num pull request bundles: " << pull_request_bundles_.size();
  result << "\n- num objects requested pull: " << object_pull_requests_.size();
  result << "\n- num objects actively being pulled: "
         << active_object_pull_requests_.size();
  return result.str();
}

}  // namespace ray
