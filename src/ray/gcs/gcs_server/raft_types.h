#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "ray/protobuf/gcs_tables.pb.h"
#include "ray/protobuf/raft.pb.h"

namespace ray {
namespace gcs {

// Forward declarations
class ConsensusState;
class ResourceView;
class ActorTable;
class TaskTable;
class NodeTable;
class PlacementGroupTable;

/// \struct FollowerInfo
/// Represents information about a follower in the Raft group
struct FollowerInfo {
  /// The match index of the follower
  int64_t match_index;

  /// The next index of the follower
  int64_t next_index;

  /// Whether the follower is syncing
  bool is_syncing;
};

/// \struct RaftConfig
/// Configuration for Raft-based coordination
struct RaftConfig {
  /// The ID of this node
  std::string node_id;

  /// The addresses of all nodes in the Raft group
  std::vector<std::string> node_addresses;

  /// The election timeout in milliseconds
  int64_t election_timeout;

  /// The heartbeat interval in milliseconds
  int64_t heartbeat_interval;

  /// The maximum number of log entries to keep
  int64_t max_log_entries;

  /// The snapshot interval in number of log entries
  int64_t snapshot_interval;
};

/// \struct StateUpdate
/// Represents a state update in the Raft group
class StateUpdate {
 public:
  /// The type of update
  enum class Type : std::uint8_t {
    RESOURCE_UPDATE,
    ACTOR_UPDATE,
    TASK_UPDATE,
    NODE_UPDATE,
    PLACEMENT_GROUP_UPDATE
  };

  /// The type of this update
  Type type;

  /// The serialized update data
  std::string data;

  /// Apply this update to the given state
  void ApplyTo(ConsensusState &state) const;
};

/// \struct ConsensusState
/// Represents the state that needs to be replicated across GCS instances
class ConsensusState {
 public:
  /// The current term
  int64_t term;

  /// The current leader ID
  std::string leader_id;

  /// The resource view
  ResourceView resource_view;

  /// The actor table
  ActorTable actor_table;

  /// The task table
  TaskTable task_table;

  /// The node table
  NodeTable node_table;

  /// The placement group table
  PlacementGroupTable placement_group_table;

  /// Serialize this state to a string
  bool SerializeToString(std::string *output) const;

  /// Parse this state from a string
  bool ParseFromString(const std::string &input);
};

}  // namespace gcs
}  // namespace ray 