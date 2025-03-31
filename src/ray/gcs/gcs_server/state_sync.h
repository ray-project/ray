#pragma once

#include <string>
#include <unordered_map>
#include <vector>

#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/status.h"

namespace ray {
namespace gcs {

class GcsServer;

// Represents a state change event
struct StateChange {
  std::string key;
  std::string value;
  int64_t version;
  std::string source_node;
  int64_t timestamp;
};

// Represents a state snapshot
struct StateSnapshot {
  std::unordered_map<std::string, std::string> state;
  int64_t version;
  std::string source_node;
  int64_t timestamp;
};

class StateSync {
 public:
  StateSync(GcsServer &gcs_server, instrumented_io_context &io_context);
  ~StateSync() = default;

  // Start/stop the state synchronization
  Status Start();
  Status Stop();

  // Update state
  Status UpdateState(const std::string &key, const std::string &value);

  // Get state value
  Status GetState(const std::string &key, std::string *value);

  // Get all state
  Status GetAllState(std::unordered_map<std::string, std::string> *state);

  // Sync state with other nodes
  Status SyncState();

  // Get current state snapshot
  Status GetSnapshot(StateSnapshot *snapshot);

  // Apply state snapshot
  Status ApplySnapshot(const StateSnapshot &snapshot);

  // Get state changes since version
  Status GetStateChangesSince(int64_t version,
                            std::vector<StateChange> *changes);

 private:
  // Apply a state change
  Status ApplyStateChange(const StateChange &change);

  // Reference to GCS server
  GcsServer &gcs_server_;

  // IO context
  instrumented_io_context &io_context_;

  // Current state
  std::unordered_map<std::string, std::string> state_;

  // Current version
  int64_t current_version_ = 0;

  // State change history
  std::vector<StateChange> change_history_;

  // Maximum history size
  const size_t max_history_size_ = 1000;
};

}  // namespace gcs
}  // namespace ray 