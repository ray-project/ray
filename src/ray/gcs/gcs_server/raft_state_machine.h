#pragma once

#include <memory>
#include <string>

#include "ray/common/status.h"
#include "ray/gcs/gcs_server/raft_types.h"
#include "ray/gcs/gcs_server/raft_storage.h"
#include "ray/gcs/gcs_server/raft_network.h"
#include "ray/protobuf/raft.pb.h"

namespace ray {
namespace gcs {

class RaftStateMachine {
 public:
  RaftStateMachine(std::shared_ptr<RaftStorage> storage,
                  std::shared_ptr<RaftNetwork> network,
                  const RaftConfig &config);

  Status Initialize();
  Status Terminate();

  Status SubmitUpdate(const StateUpdate &update);
  void SyncState(const ConsensusState &state);
  int64_t GetCurrentTerm() const;
  std::string GetLeaderId() const;
  bool IsLeader() const;

 private:
  void HandleVoteRequest(const VoteRequest &request);
  void HandleVoteResponse(const VoteResponse &response);
  void HandleAppendEntriesRequest(const AppendEntriesRequest &request);
  void HandleAppendEntriesResponse(const AppendEntriesResponse &response);
  void HandleInstallSnapshotRequest(const InstallSnapshotRequest &request);
  void HandleInstallSnapshotResponse(const InstallSnapshotResponse &response);

  void StartLeaderElection();
  void HandleLeaderElection(bool is_leader);
  void SendHeartbeats();
  void ApplyLogEntries();
  void CreateSnapshot();
  void InstallSnapshot(const Snapshot &snapshot);

  std::shared_ptr<RaftStorage> storage_;
  std::shared_ptr<RaftNetwork> network_;
  RaftConfig config_;
  int64_t current_term_;
  std::string leader_id_;
  int64_t commit_index_;
  int64_t last_applied_;
  bool is_initialized_;
  bool is_terminated_;
  std::vector<LogEntry> log_;
  ConsensusState current_state_;
  std::map<std::string, FollowerInfo> followers_;
};

}  // namespace gcs
}  // namespace ray 