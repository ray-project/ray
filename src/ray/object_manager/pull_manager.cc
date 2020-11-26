#include "ray/object_manager/pull_manager.h"

namespace ray {

PullManager::PullManager(
    NodeID &self_node_id,
    const std::function<bool(const ObjectID &)> &object_is_local,
    const std::function<void(const ObjectID &, const NodeID &)> &send_pull_request,
    const std::function<int(int)> &get_rand_int,
    const RestoreSpilledObjectCallback &restore_spilled_object)

    : self_node_id_(self_node_id),
      object_is_local_(object_is_local),
      send_pull_request_(send_pull_request),
      get_rand_int_(get_rand_int),
      restore_spilled_object_(restore_spilled_object) {}

bool PullManager::Pull(const ObjectID &object_id, const rpc::Address &owner_address) {
  RAY_LOG(DEBUG) << "Pull "
                 << " of object " << object_id;
  // Check if object is already local.
  if (object_is_local_(object_id)) {
    RAY_LOG(ERROR) << object_id << " attempted to pull an object that's already local.";
    return false;
  }
  if (pull_requests_.find(object_id) != pull_requests_.end()) {
    RAY_LOG(DEBUG) << object_id << " has inflight pull_requests, skipping.";
    return false;
  }

  pull_requests_.emplace(object_id, PullRequest());
  return true;
}

void PullManager::OnLocationChange(const ObjectID &object_id,
                                   const std::unordered_set<NodeID> &client_ids,
                                   const std::string &spilled_url) {
  // Exit if the Pull request has already been fulfilled or canceled.
  auto it = pull_requests_.find(object_id);
  if (it == pull_requests_.end()) {
    return;
  }
  // Reset the list of clients that are now expected to have the object.
  // NOTE(swang): Since we are overwriting the previous list of clients,
  // we may end up sending a duplicate request to the same client as
  // before.
  it->second.client_locations = std::vector<NodeID>(client_ids.begin(), client_ids.end());
  if (!spilled_url.empty()) {
    // Try to restore the spilled object.
    restore_spilled_object_(object_id, spilled_url,
                            [this, object_id](const ray::Status &status) {
                              // Fall back to fetching from another object manager.
                              if (!status.ok()) {
                                TryPull(object_id);
                              }
                            });
  } else if (it->second.client_locations.empty()) {
    // The object locations are now empty, so we should wait for the next
    // notification about a new object location.  Cancel the timer until
    // the next Pull attempt since there are no more clients to try.
    if (it->second.retry_timer != nullptr) {
      it->second.retry_timer->cancel();
      it->second.timer_set = false;
    }
  } else {
    // New object locations were found, so begin trying to pull from a
    // client. This will be called every time a new client location
    // appears.
    TryPull(object_id);
  }
}

void PullManager::TryPull(const ObjectID &object_id) {
  auto it = pull_requests_.find(object_id);
  if (it == pull_requests_.end()) {
    return;
  }

  auto &node_vector = it->second.client_locations;

  // The timer should never fire if there are no expected client locations.
  if (node_vector.empty()) {
    return;
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
    it->second.timer_set = false;
    return;
  }

  // Choose a random client to pull the object from.
  // Generate a random index.
  int node_index = get_rand_int_(node_vector.size());
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
}

bool PullManager::CancelPull(const ObjectID &object_id) {
  auto it = pull_requests_.find(object_id);
  if (it == pull_requests_.end()) {
    return false;
  }

  pull_requests_.erase(it);
  return true;
}

int PullManager::NumRequests() const { return pull_requests_.size(); }

}  // namespace ray
