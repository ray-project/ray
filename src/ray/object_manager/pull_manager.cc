#include "pull_manager.h"

#include <chrono>

#include "ray/ray_config.h"

namespace ray {

PullInfo::PullInfo(bool required, const ObjectID &object_id,
                   boost::asio::io_service &main_service,
                   const std::function<void()> &timer_callback)
    : required(required),
      total_num_chunks(-1),
      num_in_progress_chunk_ids(0),
      retry_timer(main_service),
      timer_callback(timer_callback) {
  RestartTimer(main_service);
}

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

void PullInfo::RestartTimer(boost::asio::io_service &main_service) {
  // Create a new timer. This will cancel the old timer.
  retry_timer = boost::asio::deadline_timer(main_service);
  boost::posix_time::milliseconds retry_timeout(
      RayConfig::instance().object_manager_pull_timeout_ms());
  retry_timer.expires_from_now(retry_timeout);
  // Pass a copy of the callback into the timer.
  auto &timer_callback_copy = timer_callback;
  retry_timer.async_wait([timer_callback_copy](const boost::system::error_code &error) {
    if (!error) {
      timer_callback_copy();
    } else {
      // Check that the error was due to the timer being canceled.
      RAY_CHECK(error == boost::asio::error::operation_aborted);
    }
  });
}

PullManager::PullManager(boost::asio::io_service &main_service, const ClientID &client_id,
                         const ObjectRequestManagementCallback &callback)
    : total_pull_calls_(0),
      total_cancel_calls_(0),
      total_successful_chunk_reads_(0),
      total_failed_chunk_reads_(0),
      main_service_(main_service),
      client_id_(client_id),
      callback_(callback),
      gen_(std::chrono::high_resolution_clock::now().time_since_epoch().count()) {}

void PullManager::ReceivePushRequest(const ObjectID &object_id, const ClientID &client_id,
                                     int64_t chunk_index, int64_t num_chunks) {
  RAY_CHECK(client_id != client_id_);
  std::vector<ClientID> clients_to_cancel;

  auto it = pulls_.find(object_id);
  if (it == pulls_.end()) {
    auto insertion_it = pulls_.insert(std::make_pair(
        object_id, std::unique_ptr<PullInfo>(
                       new PullInfo(false, object_id, main_service_,
                                    [this, object_id]() { TimerExpires(object_id); }))));

    RAY_CHECK(insertion_it.second);
    it = insertion_it.first;

    auto &pull_info = *it->second;
    RAY_CHECK(!pull_info.required);
    pull_info.client_receiving_from = client_id;
    pull_info.InitializeChunksIfNecessary(num_chunks);
  } else {
    auto &pull_info = *it->second;
    if (pull_info.client_receiving_from.is_nil()) {
      pull_info.InitializeChunksIfNecessary(num_chunks);
      pull_info.client_receiving_from = client_id;
      // Cancel it from all remote object managers that we've requested it from
      // except for this one.
      for (auto const &client_requested_from : pull_info.clients_requested_from) {
        if (client_requested_from != client_id) {
          clients_to_cancel.push_back(client_requested_from);
        }
      }
      pull_info.clients_requested_from.clear();
    } else {
      // We are already receiving the object from some other remote object
      // manager.
      RAY_CHECK(pull_info.clients_requested_from.empty());
    }
  }

  auto &pull_info = *it->second;
  RAY_CHECK(pull_info.num_in_progress_chunk_ids >= 0);
  pull_info.num_in_progress_chunk_ids++;

  // Cancel the request from everyone else.
  callback_(std::vector<ClientID>(), clients_to_cancel, /* abort */ false);
}

// If the object is required and if we are not already receiving the object from
// some object manager, request the object from any object managers that we have
// not already requested the object from.
void PullManager::NewObjectLocations(
    const ObjectID &object_id, const std::unordered_set<ClientID> &clients_with_object) {
  auto it = pulls_.find(object_id);
  if (it == pulls_.end()) {
    return;
  }

  auto &pull_info = *it->second;
  pull_info.clients_with_object = clients_with_object;

  if (!pull_info.required) {
    return;
  }

  // If we are already receiving the object, don't request it from any more
  // object managers.
  if (!pull_info.client_receiving_from.is_nil()) {
    return;
  }

  std::vector<ClientID> clients_to_request;
  for (auto const &client_id : pull_info.clients_with_object) {
    if (pull_info.clients_requested_from.count(client_id) == 0) {
      clients_to_request.push_back(client_id);
      pull_info.clients_requested_from.insert(client_id);
    }
  }

  if (!clients_to_request.empty()) {
    pull_info.RestartTimer(main_service_);
  }
  callback_(clients_to_request, std::vector<ClientID>(), /* abort */ false);
}

void PullManager::PullObject(const ObjectID &object_id) {
  ++total_pull_calls_;
  auto it = pulls_.find(object_id);

  if (it == pulls_.end()) {
    auto insertion_it = pulls_.insert(std::make_pair(
        object_id, std::unique_ptr<PullInfo>(
                       new PullInfo(true, object_id, main_service_,
                                    [this, object_id]() { TimerExpires(object_id); }))));
    it = insertion_it.first;
    auto &pull_info = *it->second;
    RAY_CHECK(pull_info.required);
    RAY_CHECK(pull_info.client_receiving_from.is_nil());
  } else {
    auto &pull_info = *it->second;

    if (!pull_info.required) {
      // In this case, we are already receiving the object, but it was not
      // required before.
      auto &pull_info = *it->second;
      RAY_CHECK(!pull_info.client_receiving_from.is_nil());
      pull_info.required = true;
    } else {
      // In this case, we are already pulling the object, so there is nothing new
      // to do.
    }
  }
}

void PullManager::CancelPullObject(const ObjectID &object_id) {
  ++total_cancel_calls_;

  auto it = pulls_.find(object_id);

  // If we currently are not trying to pull this object, then there is nothing
  // to do.
  if (it == pulls_.end()) {
    return;
  }

  auto &pull_info = *it->second;
  if (!pull_info.required) {
    // This object already was not required, so this cancel message contains no
    // new information.
    return;
  }

  pull_info.required = false;

  std::vector<ClientID> clients_to_cancel;
  for (auto const &client_id : pull_info.clients_requested_from) {
    clients_to_cancel.push_back(client_id);
  }
  if (!pull_info.client_receiving_from.is_nil()) {
    clients_to_cancel.push_back(pull_info.client_receiving_from);
  }
  pull_info.client_receiving_from = ClientID::nil();

  bool abort_creation = false;
  if (pull_info.LifetimeEnded()) {
    pulls_.erase(object_id);
    abort_creation = true;
  }
  callback_(std::vector<ClientID>(), clients_to_cancel, abort_creation);
}

/// We finished reading a chunk.
void PullManager::ChunkReadSucceeded(const ObjectID &object_id, const ClientID &client_id,
                                     int64_t chunk_index) {
  ++total_successful_chunk_reads_;
  auto it = pulls_.find(object_id);
  RAY_CHECK(it != pulls_.end());
  auto &pull_info = *it->second;

  pull_info.num_in_progress_chunk_ids--;
  RAY_CHECK(pull_info.num_in_progress_chunk_ids >= 0);
  RAY_CHECK(pull_info.remaining_chunk_ids.erase(chunk_index) == 1);
  RAY_CHECK(pull_info.received_chunk_ids.insert(chunk_index).second);

  if (client_id == pull_info.client_receiving_from) {
    pull_info.RestartTimer(main_service_);
  }

  bool abort_creation = false;
  if (pull_info.LifetimeEnded()) {
    pulls_.erase(object_id);
    abort_creation = true;
  }
  callback_(std::vector<ClientID>(), std::vector<ClientID>(), abort_creation);
}

void PullManager::ChunkReadFailed(const ObjectID &object_id, const ClientID &client_id,
                                  int64_t chunk_index) {
  ++total_failed_chunk_reads_;
  auto it = pulls_.find(object_id);
  RAY_CHECK(it != pulls_.end());
  auto &pull_info = *it->second;

  pull_info.num_in_progress_chunk_ids--;
  RAY_CHECK(pull_info.num_in_progress_chunk_ids >= 0);

  std::vector<ClientID> clients_to_cancel;
  if (client_id == pull_info.client_receiving_from &&
      pull_info.received_chunk_ids.count(chunk_index) == 0) {
    pull_info.client_receiving_from = ClientID::nil();
    clients_to_cancel.push_back(pull_info.client_receiving_from);
  }

  bool abort_creation = false;
  if (pull_info.LifetimeEnded()) {
    pulls_.erase(object_id);
    abort_creation = true;
  }
  callback_(std::vector<ClientID>(), clients_to_cancel, abort_creation);
}

// The timer for this pull request expired. If the object is required, then we
// need to reissue some new requests. If it is not required, then we may need to
// end the pull lifetime.
void PullManager::TimerExpires(const ObjectID &object_id) {
  auto it = pulls_.find(object_id);
  RAY_CHECK(it != pulls_.end());
  auto &pull_info = *it->second;

  std::vector<ClientID> clients_to_request;
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
      clients_to_request.push_back(client_id);
    }
  }

  if (pull_info.required) {
    pull_info.RestartTimer(main_service_);
  }

  bool abort_creation = false;
  if (pull_info.LifetimeEnded()) {
    pulls_.erase(object_id);
    abort_creation = true;
  }
  callback_(clients_to_request, std::vector<ClientID>(), abort_creation);
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
