#include "ray/object_manager/pull_manager.h"

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

bool PullManager::Pull(const ObjectID &object_id, const rpc::Address &owner_address) {
  RAY_LOG(DEBUG) << "Pull "
                 << " of object " << object_id;
  // Check if object is already local.
  if (object_is_local_(object_id)) {
    RAY_LOG(DEBUG) << object_id << " attempted to pull an object that's already local.";
    return false;
  }
  if (pull_requests_.find(object_id) != pull_requests_.end()) {
    RAY_LOG(DEBUG) << object_id << " has inflight pull_requests, skipping.";
    return false;
  }

  pull_requests_.emplace(object_id, PullRequest(get_time_() + pull_timeout_ms_ / 1000));
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
  auto &request = it->second;
  // Reset the list of clients that are now expected to have the object.
  // NOTE(swang): Since we are overwriting the previous list of clients,
  // we may end up sending a duplicate request to the same client as
  // before.
  it->second.client_locations = std::vector<NodeID>(client_ids.begin(), client_ids.end());
  if (request.inflight_pulls.size() == 0) {
    if (!spilled_url.empty()) {
      RAY_LOG(DEBUG) << "OnLocationChange " << spilled_url << " num clients "
                     << client_ids.size();
      // Try to restore the spilled object.
      restore_spilled_object_(object_id, spilled_url,
                              [this, object_id](const ray::Status &status) {
                                // Fall back to fetching from another object manager.
                                if (!status.ok()) {
                                  TryPull(object_id);
                                }
                              });
    } else {
      // New object locations were found, so begin trying to pull from a
      // client. This will be called every time a new client location
      // appears.
      TryPull(object_id);
    }
  }
}

void PullManager::TryPull(const ObjectID &object_id) {
  auto it = pull_requests_.find(object_id);
  if (it == pull_requests_.end()) {
    return;
  }

  auto &node_vector = it->second.client_locations;
  auto &inflight_pulls = it->second.inflight_pulls;

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
    return;
  }

  NodeID node_id;
  int num_candidates = 0;
  for (NodeID potential : node_vector) {
    if (inflight_pulls.count(potential) > 0) {
      continue;
    }
    num_candidates++;
    std::uniform_int_distribution<int> distribution(0, num_candidates - 1);
    if (distribution(gen_) == 0) {
      node_id = potential;
    }
  }

  if (node_id.IsNil()) {
    RAY_LOG(DEBUG)
        << "Resending pull request for " << object_id << " to " << node_id
        << " this means we've already tried pulling the object from all available nodes.";
    // We've already tried all the nodes that we know about. Clear the set of inflight
    // nodes and try again.
    inflight_pulls.clear();
    // Choose a random client to pull the object from.
    // Generate a random index.
    std::uniform_int_distribution<int> distribution(0, node_vector.size() - 1);
    int node_index = distribution(gen_);
    node_id = node_vector[node_index];
    // If the object manager somehow ended up choosing itself, choose a different
    // object manager.
    if (node_id == self_node_id_) {
      std::swap(node_vector[node_index], node_vector[node_vector.size() - 1]);
      node_vector.pop_back();
      RAY_LOG(WARNING)
          << "The object manager with ID " << self_node_id_
          << " is trying to pull object " << object_id
          << " but the object table suggests that this object manager "
          << "already has the object. It is most likely due to memory pressure, object "
          << "pull has been requested before object location is updated.";
      node_id = node_vector[node_index % node_vector.size()];
      RAY_CHECK(node_id != self_node_id_);
    }
  }

  RAY_LOG(DEBUG) << "Sending pull request from " << self_node_id_ << " to " << node_id
                 << " of object " << object_id;
  const auto time = get_time_();
  auto &request = it->second;
  auto retry_timeout_len = (pull_timeout_ms_ / 1000.) * (1UL << request.num_retries);
  request.next_pull_time = time + retry_timeout_len;
  request.inflight_pulls.insert(node_id);
  send_pull_request_(object_id, node_id);
}

bool PullManager::CancelPull(const ObjectID &object_id) {
  auto it = pull_requests_.find(object_id);
  if (it == pull_requests_.end()) {
    return false;
  }

  pull_requests_.erase(it);
  return true;
}

void PullManager::Tick() {
  for (auto &pair : pull_requests_) {
    const auto &object_id = pair.first;
    auto &request = pair.second;
    const auto time = get_time_();
    if (time >= request.next_pull_time) {
      TryPull(object_id);
      // Bound the retry time at 10 * 1024 seconds.
      request.num_retries = std::min(request.num_retries + 1, 10);
    }
  }
}

int PullManager::NumActiveRequests() const { return pull_requests_.size(); }

}  // namespace ray
