#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/status.h"
#include "ray/gcs/gcs_server/gcs_server.h"
#include "ray/rpc/raft/raft_client.h"
#include "ray/protobuf/raft.pb.h"

namespace ray {
namespace gcs {

/// RaftCoordinator manages the Raft protocol state and coordinates with the GCS server.
class RaftCoordinator {
 public:
  /// Constructor.
  ///
  /// \param[in] gcs_server The GCS server instance.
  /// \param[in] io_context The IO context for async operations.
  RaftCoordinator(GcsServer &gcs_server, instrumented_io_context &io_context)
      : gcs_server_(gcs_server),
        io_context_(io_context),
        current_term_(0),
        voted_for_(""),
        commit_index_(0),
        last_applied_(0),
        state_(RaftState::FOLLOWER),
        log_(),
        next_index_(),
        match_index_() {}

  /// Handle a RequestVote RPC.
  ///
  /// \param[in] request The RequestVote request.
  /// \param[out] reply The RequestVote reply.
  /// \return Status indicating success or failure.
  Status HandleRequestVote(const rpc::RequestVoteRequest &request,
                          rpc::RequestVoteReply *reply);

  /// Handle an AppendEntries RPC.
  ///
  /// \param[in] request The AppendEntries request.
  /// \param[out] reply The AppendEntries reply.
  /// \return Status indicating success or failure.
  Status HandleAppendEntries(const rpc::AppendEntriesRequest &request,
                            rpc::AppendEntriesReply *reply);

  /// Handle an InstallSnapshot RPC.
  ///
  /// \param[in] request The InstallSnapshot request.
  /// \param[out] reply The InstallSnapshot reply.
  /// \return Status indicating success or failure.
  Status HandleInstallSnapshot(const rpc::InstallSnapshotRequest &request,
                              rpc::InstallSnapshotReply *reply);

  /// Start the Raft coordinator.
  void Start();

  /// Stop the Raft coordinator.
  void Stop();

 private:
  /// The possible states of a Raft node.
  enum class RaftState {
    FOLLOWER,
    CANDIDATE,
    LEADER
  };

  /// Start a new election.
  void StartElection();

  /// Send heartbeat to all followers.
  void SendHeartbeat();

  /// Update the commit index and apply committed entries.
  void UpdateCommitIndex();

  /// Reference to the GCS server.
  GcsServer &gcs_server_;

  /// The IO context for async operations.
  instrumented_io_context &io_context_;

  /// Current term number.
  int64_t current_term_;

  /// ID of the candidate voted for in current term.
  std::string voted_for_;

  /// Index of highest log entry known to be committed.
  int64_t commit_index_;

  /// Index of highest log entry applied to state machine.
  int64_t last_applied_;

  /// Current Raft state.
  RaftState state_;

  /// Map of node ID to Raft client.
  std::unordered_map<std::string, std::unique_ptr<rpc::RaftClient>> raft_clients_;

  /// Timer for election timeout.
  std::unique_ptr<boost::asio::deadline_timer> election_timer_;

  /// Timer for heartbeat.
  std::unique_ptr<boost::asio::deadline_timer> heartbeat_timer_;

  /// Log entries.
  std::vector<rpc::RaftLogEntry> log_;

  /// For each server, index of the next log entry to send to that server.
  std::unordered_map<std::string, int64_t> next_index_;

  /// For each server, index of highest log entry known to be replicated on that server.
  std::unordered_map<std::string, int64_t> match_index_;

  /// Get the last log term.
  int64_t GetLastLogTerm() const {
    return log_.empty() ? 0 : log_.back().term();
  }

  /// Get the last log index.
  int64_t GetLastLogIndex() const {
    return log_.empty() ? 0 : log_.size() - 1;
  }

  /// Check if the log contains an entry at the given index with the given term.
  bool CheckLogEntry(int64_t index, int64_t term) const {
    return index < log_.size() && log_[index].term() == term;
  }

  /// Apply a log entry to the state machine.
  void ApplyLogEntry(const rpc::RaftLogEntry &entry);

  /// Truncate the log to the given index.
  void TruncateLog(int64_t index);

  /// Install a snapshot.
  void InstallSnapshot(const rpc::InstallSnapshotRequest &request);
};

}  // namespace gcs
}  // namespace ray 