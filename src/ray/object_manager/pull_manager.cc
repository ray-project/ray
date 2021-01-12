#include "ray/object_manager/pull_manager.h"

#include "ray/common/common_protocol.h"

namespace ray {

PullManager::PullManager(
    NodeID &self_node_id, const std::function<bool(const ObjectID &)> object_is_local,
    const std::function<void(const ObjectID &, const NodeID &)> send_pull_request,
    const RestoreSpilledObjectCallback restore_spilled_object,
    const std::function<double()> get_time, int pull_timeout_ms)
    : self_node_id_(self_node_id),
      object_is_local_(object_is_local),
      send_pull_request_(send_pull_request),
      restore_spilled_object_(restore_spilled_object),
      get_time_(get_time),
      pull_timeout_ms_(pull_timeout_ms),
      gen_(std::chrono::high_resolution_clock::now().time_since_epoch().count()) {}

uint64_t PullManager::Pull(const std::vector<rpc::ObjectReference> &object_ref_bundle,
                           std::vector<rpc::ObjectReference> *objects_to_locate) {
  auto bundle_it = pull_request_bundles_.emplace(next_req_id_++, object_ref_bundle).first;
  RAY_LOG(DEBUG) << "Start pull request " << bundle_it->first;

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
    }
    it->second.bundle_request_ids.insert(bundle_it->first);
  }

  return bundle_it->first;
}

std::vector<ObjectID> PullManager::CancelPull(uint64_t request_id) {
  std::vector<ObjectID> objects_to_cancel;
  RAY_LOG(DEBUG) << "Cancel pull request " << request_id;
  auto bundle_it = pull_request_bundles_.find(request_id);
  RAY_CHECK(bundle_it != pull_request_bundles_.end());

  for (const auto &ref : bundle_it->second) {
    auto obj_id = ObjectRefToId(ref);
    auto it = object_pull_requests_.find(obj_id);
    RAY_CHECK(it != object_pull_requests_.end());
    RAY_CHECK(it->second.bundle_request_ids.erase(request_id));
    if (it->second.bundle_request_ids.empty()) {
      object_pull_requests_.erase(it);
      objects_to_cancel.push_back(obj_id);
    }
  }

  pull_request_bundles_.erase(bundle_it);
  return objects_to_cancel;
}

void PullManager::OnLocationChange(const ObjectID &object_id,
                                   const std::unordered_set<NodeID> &client_ids,
                                   const std::string &spilled_url) {
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
  RAY_LOG(DEBUG) << "OnLocationChange " << spilled_url << " num clients "
                 << client_ids.size();

  TryToMakeObjectLocal(object_id);
}

void PullManager::TryToMakeObjectLocal(const ObjectID &object_id) {
  if (object_is_local_(object_id)) {
    return;
  }
  auto it = object_pull_requests_.find(object_id);
  if (it == object_pull_requests_.end()) {
    return;
  }
  auto &request = it->second;
  if (request.next_pull_time > get_time_()) {
    return;
  }

  if (!request.spilled_url.empty()) {
    // Try to restore the spilled object.
    restore_spilled_object_(
        object_id, request.spilled_url, [this, object_id](const ray::Status &status) {
          bool did_pull = true;
          // Fall back to fetching from another object manager.
          if (!status.ok()) {
            did_pull = PullFromRandomLocation(object_id);
          }
          if (!did_pull) {
            RAY_LOG(WARNING) << "Object restoration failed and the object could not be "
                                "found on any other nodes. Object id: "
                             << object_id;
          }
        });
    UpdateRetryTimer(request);
  } else {
    // New object locations were found, so begin trying to pull from a
    // client. This will be called every time a new client location
    // appears.
    bool did_pull = PullFromRandomLocation(object_id);
    if (did_pull) {
      UpdateRetryTimer(request);
    }
  }
}

bool PullManager::PullFromRandomLocation(const ObjectID &object_id) {
  auto it = object_pull_requests_.find(object_id);
  if (it == object_pull_requests_.end()) {
    return false;
  }

  auto &node_vector = it->second.client_locations;

  // The timer should never fire if there are no expected client locations.
  if (node_vector.empty()) {
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

void PullManager::UpdateRetryTimer(ObjectPullRequest &request) {
  const auto time = get_time_();
  auto retry_timeout_len = (pull_timeout_ms_ / 1000.) * (1UL << request.num_retries);
  request.next_pull_time = time + retry_timeout_len;

  // Bound the retry time at 10 * 1024 seconds.
  request.num_retries = std::min(request.num_retries + 1, 10);
}

void PullManager::Tick() {
  for (auto &pair : object_pull_requests_) {
    const auto &object_id = pair.first;
    TryToMakeObjectLocal(object_id);
  }
}

int PullManager::NumActiveRequests() const { return object_pull_requests_.size(); }

}  // namespace ray
