#pragma once

#include <functional>
#include <memory>
#include <string>
#include <vector>
#include <map>

#include "ray/common/asio/asio_util.h"
#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/status.h"
#include "ray/gcs/gcs_server/raft_types.h"
#include "ray/protobuf/raft.pb.h"
#include "ray/util/logging.h"

namespace ray {
namespace gcs {

/// \class RaftGroup
/// Implements the Raft consensus algorithm using Seastar
template <typename State>
class RaftGroup {
 public:
  /// \brief Constructor
  /// \param election_timeout The election timeout in milliseconds
  /// \param heartbeat_interval The heartbeat interval in milliseconds
  /// \param leader_callback Callback when leadership changes
  RaftGroup(int64_t election_timeout,
            int64_t heartbeat_interval,
            std::function<void(bool)> leader_callback);

  /// \brief Start the Raft group
  void Start();

  /// \brief Stop the Raft group
  void Stop();

  /// \brief Submit a state update
  /// \param update The state update to submit
  /// \return Status indicating success or failure
  Status SubmitUpdate(const StateUpdate &update);

  /// \brief Sync state with followers
  /// \param state The current state to sync
  void SyncState(const State &state);

  /// \brief Get the current term
  /// \return The current term
  int64_t GetCurrentTerm() const;

  /// \brief Get the current leader ID
  /// \return The current leader ID
  std::string GetLeaderId() const;

  /// \brief Check if this instance is the leader
  /// \return True if this instance is the leader
  bool IsLeader() const;

 private:
  /// The election timeout
  int64_t election_timeout_;

  /// The heartbeat interval
  int64_t heartbeat_interval_;

  /// The leader callback
  std::function<void(bool)> leader_callback_;

  /// The current term
  int64_t current_term_;

  /// The current leader ID
  std::string leader_id_;

  /// The log entries
  std::vector<LogEntry> log_;

  /// The current state
  State current_state_;

  /// The commit index
  int64_t commit_index_;

  /// The last applied index
  int64_t last_applied_;

  /// Whether the group is started
  bool is_started_;

  /// Whether the group is stopped
  bool is_stopped_;

  /// The followers
  std::map<std::string, FollowerInfo> followers_;

  /// The Raft configuration
  RaftConfig config_;

  /// The IO service
  instrumented_io_context io_service_;

  /// Start leader election
  void StartLeaderElection();

  /// Handle leader election results
  void HandleLeaderElection(bool is_leader);

  /// Send heartbeats to followers
  void SendHeartbeats();

  /// Handle vote requests
  void HandleVoteRequest(const VoteRequest &request);

  /// Handle vote responses
  void HandleVoteResponse(const VoteResponse &response);

  /// Handle append entries requests
  void HandleAppendEntriesRequest(const AppendEntriesRequest &request);

  /// Handle append entries responses
  void HandleAppendEntriesResponse(const AppendEntriesResponse &response);

  /// Handle install snapshot requests
  void HandleInstallSnapshotRequest(const InstallSnapshotRequest &request);

  /// Handle install snapshot responses
  void HandleInstallSnapshotResponse(const InstallSnapshotResponse &response);

  /// Apply log entries
  void ApplyLogEntries();

  /// Create snapshot
  void CreateSnapshot();

  /// Install snapshot
  void InstallSnapshot(const Snapshot &snapshot);
};

/// \class LeaderElection
/// Implements leader election using Seastar
class LeaderElection {
 public:
  /// \brief Constructor
  /// \param election_timeout The election timeout in milliseconds
  /// \param leader_callback Callback when leadership changes
  LeaderElection(int64_t election_timeout,
                 std::function<void(bool)> leader_callback);

  /// \brief Start leader election
  void Start();

  /// \brief Stop leader election
  void Stop();

  /// \brief Get the current term
  /// \return The current term
  int64_t GetCurrentTerm() const;

  /// \brief Get the current leader ID
  /// \return The current leader ID
  std::string GetLeaderId() const;

  /// \brief Check if this instance is the leader
  /// \return True if this instance is the leader
  bool IsLeader() const;

 private:
  /// The election timeout
  int64_t election_timeout_;

  /// The leader callback
  std::function<void(bool)> leader_callback_;

  /// The current term
  int64_t current_term_;

  /// The current leader ID
  std::string leader_id_;

  /// The number of votes received
  int64_t votes_received_;

  /// The number of votes needed
  int64_t votes_needed_;

  /// Whether election is started
  bool is_started_;

  /// Whether election is stopped
  bool is_stopped_;

  /// Start election timer
  void StartElectionTimer();

  /// Handle election timeout
  void HandleElectionTimeout();

  /// Request votes
  void RequestVotes();

  /// Handle vote requests
  void HandleVoteRequest(const VoteRequest &request);

  /// Handle vote responses
  void HandleVoteResponse(const VoteResponse &response);
};

}  // namespace gcs
}  // namespace ray 