#include "pull_manager.h"

#include <chrono>

#include "ray/ray_config.h"

namespace ray {

PullInfo::PullInfo(bool required, const ObjectID &object_id)
    : required(required),
      total_num_chunks(-1),
      num_in_progress_chunk_ids(0) {}

void PullInfo::InitializeChunksIfNecessary(int64_t num_chunks) {
  if (total_num_chunks == -1) {
    total_num_chunks = num_chunks;
    RAY_CHECK(received_chunk_ids.size() == 0);
    RAY_CHECK(num_in_progress_chunk_ids == 0);
    for (int64_t i = 0; i < total_num_chunks; ++i) {
      remaining_chunk_ids.insert(i);
    }
  }
}

bool PullInfo::LifetimeEnded() {
  return !required && client_receiving_from.is_nil() && num_in_progress_chunk_ids == 0;
}

PullManager::PullManager(const ClientID &client_id)
    : total_pull_calls_(0),
      total_cancel_calls_(0),
      total_successful_chunk_reads_(0),
      total_failed_chunk_reads_(0),
      client_id_(client_id),
      gen_(std::chrono::high_resolution_clock::now().time_since_epoch().count()) {}

void PullManager::ReceivePushRequest(const ObjectID &object_id, const ClientID &client_id,
                                     int64_t chunk_index, int64_t num_chunks,
                                     std::vector<ClientID> *clients_to_cancel,
                                     bool *start_timer) {
  *start_timer = false;
  RAY_CHECK(client_id != client_id_);
  *clients_to_cancel = std::vector<ClientID>();

  auto it = pulls_.find(object_id);
  if (it == pulls_.end()) {
    *start_timer = true;
    auto insertion_it = pulls_.insert(std::make_pair(
        object_id, PullInfo(false, object_id)));

    RAY_CHECK(insertion_it.second);
    it = insertion_it.first;

    auto &pull_info = it->second;
    RAY_CHECK(!pull_info.required);
    pull_info.client_receiving_from = client_id;
    pull_info.InitializeChunksIfNecessary(num_chunks);
  } else {
    auto &pull_info = it->second;
    if (pull_info.client_receiving_from.is_nil()) {
      pull_info.InitializeChunksIfNecessary(num_chunks);
      pull_info.client_receiving_from = client_id;
      // Cancel it from all remote object managers that we've requested it from
      // except for this one.
      for (auto const &client_requested_from : pull_info.clients_requested_from) {
        if (client_requested_from != client_id) {
          clients_to_cancel->push_back(client_requested_from);
        }
      }
      pull_info.clients_requested_from.clear();
    } else {
      // We are already receiving the object from some other remote object
      // manager.
      RAY_CHECK(pull_info.clients_requested_from.empty());
    }
  }

  auto &pull_info = it->second;
  RAY_CHECK(pull_info.num_in_progress_chunk_ids >= 0);
  pull_info.num_in_progress_chunk_ids++;
}

// If the object is required and if we are not already receiving the object from
// some object manager, request the object from any object managers that we have
// not already requested the object from.
void PullManager::NewObjectLocations(
    const ObjectID &object_id, const std::unordered_set<ClientID> &clients_with_object,
    std::vector<ClientID> *clients_to_request, bool *restart_timer) {
  *clients_to_request = std::vector<ClientID>();
  *restart_timer = false;

  auto it = pulls_.find(object_id);
  if (it == pulls_.end()) {
    return;
  }

  auto &pull_info = it->second;
  pull_info.clients_with_object = clients_with_object;

  if (!pull_info.required) {
    return;
  }

  // If we are already receiving the object, don't request it from any more
  // object managers.
  if (!pull_info.client_receiving_from.is_nil()) {
    return;
  }

  for (auto const &client_id : pull_info.clients_with_object) {
    if (pull_info.clients_requested_from.count(client_id) == 0) {
      clients_to_request->push_back(client_id);
      pull_info.clients_requested_from.insert(client_id);
    }
  }

  if (!clients_to_request->empty()) {
    *restart_timer = true;
  }
}

void PullManager::PullObject(const ObjectID &object_id, bool *subscribe_to_locations,
                             bool *start_timer) {
  ++total_pull_calls_;
  // The subscribe_to_locations variable should be set to true precisely when
  // the object becomes "required" for the first time.
  *subscribe_to_locations = true;
  *start_timer = false;
  auto it = pulls_.find(object_id);

  if (it == pulls_.end()) {
    *start_timer = true;
    auto insertion_it = pulls_.insert(std::make_pair(
        object_id, PullInfo(true, object_id)));
    it = insertion_it.first;
    auto &pull_info = it->second;
    RAY_CHECK(pull_info.required);
    RAY_CHECK(pull_info.client_receiving_from.is_nil());
  } else {
    auto &pull_info = it->second;

    if (!pull_info.required) {
      // In this case, we are already receiving the object, but it was not
      // required before.
      auto &pull_info = it->second;
      RAY_CHECK(!pull_info.client_receiving_from.is_nil());
      pull_info.required = true;
    } else {
      // In this case, we are already pulling the object, so there is nothing new
      // to do.
      *subscribe_to_locations = false;
    }
  }
}

void PullManager::CancelPullObject(const ObjectID &object_id,
                                   std::vector<ClientID> *clients_to_cancel,
                                   bool *unsubscribe_from_locations) {
  ++total_cancel_calls_;
  *clients_to_cancel = std::vector<ClientID>();
  *unsubscribe_from_locations = false;

  auto it = pulls_.find(object_id);

  // If we currently are not trying to pull this object, then there is nothing
  // to do.
  if (it == pulls_.end()) {
    return;
  }

  auto &pull_info = it->second;
  if (!pull_info.required) {
    // This object already was not required, so this cancel message contains no
    // new information.
    return;
  }

  *unsubscribe_from_locations = true;
  pull_info.required = false;

  for (auto const &client_id : pull_info.clients_requested_from) {
    clients_to_cancel->push_back(client_id);
  }
  if (!pull_info.client_receiving_from.is_nil()) {
    clients_to_cancel->push_back(pull_info.client_receiving_from);
  }
  pull_info.client_receiving_from = ClientID::nil();

  bool abort_creation = false;
  if (pull_info.LifetimeEnded()) {
    pulls_.erase(object_id);
    abort_creation = true;
  }
}

// We finished reading a chunk.
void PullManager::ChunkReadSucceeded(const ObjectID &object_id, const ClientID &client_id,
                                int64_t chunk_index, bool *abort_creation,
                                bool *restart_timer) {
  *restart_timer = false;
  ++total_successful_chunk_reads_;
  auto it = pulls_.find(object_id);
  RAY_CHECK(it != pulls_.end());
  auto &pull_info = it->second;

  pull_info.num_in_progress_chunk_ids--;
  RAY_CHECK(pull_info.num_in_progress_chunk_ids >= 0);
  RAY_CHECK(pull_info.remaining_chunk_ids.erase(chunk_index) == 1);
  RAY_CHECK(pull_info.received_chunk_ids.insert(chunk_index).second);

  if (client_id == pull_info.client_receiving_from) {
    *restart_timer = true;
  }

  *abort_creation = false;
  if (pull_info.LifetimeEnded()) {
    pulls_.erase(object_id);
    *abort_creation = true;
  }
}

void PullManager::ChunkReadFailed(
    const ObjectID &object_id, const ClientID &client_id, int64_t chunk_index,
    std::vector<ClientID> *clients_to_cancel, bool *abort_creation) {
  ++total_failed_chunk_reads_;
  auto it = pulls_.find(object_id);
  RAY_CHECK(it != pulls_.end());
  auto &pull_info = it->second;

  pull_info.num_in_progress_chunk_ids--;
  RAY_CHECK(pull_info.num_in_progress_chunk_ids >= 0);

  *clients_to_cancel = std::vector<ClientID>();
  if (client_id == pull_info.client_receiving_from &&
      pull_info.received_chunk_ids.count(chunk_index) == 0) {
    pull_info.client_receiving_from = ClientID::nil();
    clients_to_cancel->push_back(pull_info.client_receiving_from);
  }

  *abort_creation = false;
  if (pull_info.LifetimeEnded()) {
    pulls_.erase(object_id);
    *abort_creation = true;
  }
}

// The timer for this pull request expired. If the object is required, then we
// need to reissue some new requests. If it is not required, then we may need to
// end the pull lifetime.
void PullManager::TimerExpired(const ObjectID &object_id,
                               std::vector<ClientID> *clients_to_request,
                               bool *abort_creation, bool *restart_timer) {
  auto it = pulls_.find(object_id);
  RAY_CHECK(it != pulls_.end());
  auto &pull_info = it->second;

  *clients_to_request = std::vector<ClientID>();
  if (!pull_info.client_receiving_from.is_nil()) {
    // We could optionally send a cancellation message to this remote object
    // manager.
    pull_info.client_receiving_from = ClientID::nil();
  }

  if (pull_info.required && !pull_info.clients_with_object.empty()) {
    // Issue a new request for the object.
    for (auto const &client_id : pull_info.clients_with_object) {
      if (client_id == client_id_) {
        RAY_LOG(WARNING) << "This object manager already has the object "
                         << "according to the object table.";
        continue;
      }
      clients_to_request->push_back(client_id);
    }
  }

  if (pull_info.required) {
    *restart_timer = true;
  }

  if (pull_info.LifetimeEnded()) {
    pulls_.erase(object_id);
    *abort_creation = true;
  } else {
    *abort_creation = false;
  }
}

std::string PullManager::DebugString() const {
  std::stringstream result;
  result << "PullManager:";
  result << "\n- num pulls: " << pulls_.size();
  result << "\n- total pull calls " << total_pull_calls_;
  result << "\n- total cancel calls: " << total_cancel_calls_;
  result << "\n- total successful chunk reads: " << total_successful_chunk_reads_;
  result << "\n- total failed chunk reads: " << total_failed_chunk_reads_;
  return result.str();
}

}  // namespace ray
