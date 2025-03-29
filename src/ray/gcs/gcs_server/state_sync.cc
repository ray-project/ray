#include "ray/gcs/gcs_server/state_sync.h"

#include <algorithm>
#include <chrono>
#include <thread>

#include "ray/common/status.h"
#include "ray/gcs/gcs_server/gcs_server.h"
#include "ray/rpc/head_node_service.h"

namespace ray {
namespace gcs {

StateSync::StateSync(GcsServer &gcs_server,
                    instrumented_io_context &io_context)
    : gcs_server_(gcs_server), io_context_(io_context) {}

Status StateSync::Start() {
  // Initialize state
  state_.clear();
  change_history_.clear();
  current_version_ = 0;
  return Status::OK();
}

Status StateSync::Stop() {
  // Clear state
  state_.clear();
  change_history_.clear();
  current_version_ = 0;
  return Status::OK();
}

Status StateSync::UpdateState(const std::string &key,
                            const std::string &value) {
  // Create state change
  StateChange change;
  change.key = key;
  change.value = value;
  change.version = ++current_version_;
  change.source_node = gcs_server_.GetSelfNodeId();
  change.timestamp = std::chrono::system_clock::now().time_since_epoch().count();

  // Apply state change
  RAY_RETURN_NOT_OK(ApplyStateChange(change));

  return Status::OK();
}

Status StateSync::GetState(const std::string &key,
                         std::string *value) {
  auto it = state_.find(key);
  if (it == state_.end()) {
    return Status::NotFound("Key not found");
  }

  *value = it->second;
  return Status::OK();
}

Status StateSync::GetAllState(std::unordered_map<std::string, std::string> *state) {
  *state = state_;
  return Status::OK();
}

Status StateSync::SyncState() {
  // Get current snapshot
  StateSnapshot snapshot;
  RAY_RETURN_NOT_OK(GetSnapshot(&snapshot));

  // TODO: Send snapshot to other nodes
  // For now, we just return OK

  return Status::OK();
}

Status StateSync::GetSnapshot(StateSnapshot *snapshot) {
  snapshot->state = state_;
  snapshot->version = current_version_;
  snapshot->source_node = gcs_server_.GetSelfNodeId();
  snapshot->timestamp = std::chrono::system_clock::now().time_since_epoch().count();
  return Status::OK();
}

Status StateSync::ApplySnapshot(const StateSnapshot &snapshot) {
  // Only apply snapshot if it's newer than our current state
  if (snapshot.version <= current_version_) {
    return Status::OK();
  }

  // Update state
  state_ = snapshot.state;
  current_version_ = snapshot.version;

  // Clear change history as it's no longer relevant
  change_history_.clear();

  return Status::OK();
}

Status StateSync::GetStateChangesSince(int64_t version,
                                     std::vector<StateChange> *changes) {
  changes->clear();

  // Find changes after the specified version
  for (const auto &change : change_history_) {
    if (change.version > version) {
      changes->push_back(change);
    }
  }

  return Status::OK();
}

Status StateSync::ApplyStateChange(const StateChange &change) {
  // Update state
  state_[change.key] = change.value;

  // Add to history
  change_history_.push_back(change);

  // Trim history if needed
  if (change_history_.size() > max_history_size_) {
    change_history_.erase(change_history_.begin());
  }

  return Status::OK();
}

}  // namespace gcs
}  // namespace ray 