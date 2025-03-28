#include "ray/gcs/gcs_server/raft_state_machine.h"

#include <memory>
#include <string>

#include "ray/common/status.h"
#include "ray/gcs/gcs_server/raft_types.h"
#include "ray/gcs/gcs_server/raft_storage.h"
#include "ray/gcs/gcs_server/raft_network.h"
#include "ray/protobuf/raft.pb.h"
#include "ray/util/logging.h"

namespace ray {
namespace gcs {

RaftStateMachine::RaftStateMachine(std::shared_ptr<RaftStorage> storage,
                                 std::shared_ptr<RaftNetwork> network,
                                 const RaftConfig &config)
    : storage_(storage),
      network_(network),
      config_(config),
      current_term_(0),
      commit_index_(-1),
      last_applied_(-1),
      is_initialized_(false),
      is_terminated_(false) {
  // Set up network callbacks
  network_->SetVoteRequestCallback(
      [this](const VoteRequest &request) {
        HandleVoteRequest(request);
      });
  network_->SetVoteResponseCallback(
      [this](const VoteResponse &response) {
        HandleVoteResponse(response);
      });
  network_->SetAppendEntriesRequestCallback(
      [this](const AppendEntriesRequest &request) {
        HandleAppendEntriesRequest(request);
      });
  network_->SetAppendEntriesResponseCallback(
      [this](const AppendEntriesResponse &response) {
        HandleAppendEntriesResponse(response);
      });
  network_->SetInstallSnapshotRequestCallback(
      [this](const InstallSnapshotRequest &request) {
        HandleInstallSnapshotRequest(request);
      });
  network_->SetInstallSnapshotResponseCallback(
      [this](const InstallSnapshotResponse &response) {
        HandleInstallSnapshotResponse(response);
      });
}

Status RaftStateMachine::Initialize() {
  if (is_initialized_) {
    return Status::OK();
  }

  // Initialize storage
  auto status = storage_->Initialize();
  if (!status.ok()) {
    return status;
  }

  // Load persistent state
  int64_t term;
  status = storage_->LoadCurrentTerm(&term);
  if (!status.ok()) {
    return status;
  }
  current_term_ = term;

  std::string voted_for;
  status = storage_->LoadVotedFor(&voted_for);
  if (!status.ok()) {
    return status;
  }
  leader_id_ = voted_for;

  std::vector<LogEntry> entries;
  status = storage_->LoadLogEntries(&entries);
  if (!status.ok()) {
    return status;
  }
  log_ = entries;

  Snapshot snapshot;
  status = storage_->LoadSnapshot(&snapshot);
  if (!status.ok()) {
    return status;
  }
  InstallSnapshot(snapshot);

  // Initialize network
  status = network_->Initialize();
  if (!status.ok()) {
    return status;
  }

  is_initialized_ = true;
  return Status::OK();
}

Status RaftStateMachine::Terminate() {
  if (!is_initialized_ || is_terminated_) {
    return Status::OK();
  }

  // Stop network
  auto status = network_->Terminate();
  if (!status.ok()) {
    return status;
  }

  // Stop storage
  status = storage_->Terminate();
  if (!status.ok()) {
    return status;
  }

  is_terminated_ = true;
  return Status::OK();
}

Status RaftStateMachine::SubmitUpdate(const StateUpdate &update) {
  if (!is_initialized_ || is_terminated_) {
    return Status::Invalid("State machine not initialized or terminated");
  }

  if (!IsLeader()) {
    return Status::Invalid("Not the leader");
  }

  // Create log entry
  LogEntry entry;
  entry.set_term(current_term_);
  entry.set_index(log_.size());
  entry.mutable_update()->CopyFrom(update);

  // Append to log
  log_.push_back(entry);

  // Save log entry
  auto status = storage_->SaveLogEntry(entry);
  if (!status.ok()) {
    return status;
  }

  // Replicate to followers
  for (const auto &follower : followers_) {
    AppendEntriesRequest request;
    request.set_term(current_term_);
    request.set_leader_id(leader_id_);
    request.set_prev_log_index(log_.size() - 2);
    request.set_prev_log_term(log_.size() > 1 ? log_[log_.size() - 2].term() : 0);
    request.set_leader_commit(commit_index_);
    request.add_entries()->CopyFrom(entry);

    network_->SendAppendEntriesRequest(follower.first, request);
  }

  return Status::OK();
}

void RaftStateMachine::SyncState(const ConsensusState &state) {
  if (!is_initialized_ || is_terminated_ || !IsLeader()) {
    return;
  }

  // Create snapshot if needed
  if (log_.size() >= config_.snapshot_interval) {
    CreateSnapshot();
  }

  // Send heartbeats to followers
  SendHeartbeats();
}

int64_t RaftStateMachine::GetCurrentTerm() const {
  return current_term_;
}

std::string RaftStateMachine::GetLeaderId() const {
  return leader_id_;
}

bool RaftStateMachine::IsLeader() const {
  return leader_id_ == config_.node_id;
}

void RaftStateMachine::HandleVoteRequest(const VoteRequest &request) {
  if (!is_initialized_ || is_terminated_) {
    return;
  }

  if (request.term() < current_term_) {
    VoteResponse response;
    response.set_term(current_term_);
    response.set_vote_granted(false);
    network_->SendVoteResponse(request.candidate_id(), response);
    return;
  }

  if (request.term() > current_term_) {
    current_term_ = request.term();
    leader_id_ = "";
    storage_->SaveCurrentTerm(current_term_);
  }

  VoteResponse response;
  response.set_term(current_term_);

  // Grant vote if candidate's log is at least as up-to-date as ours
  if (request.last_log_term() > log_.back().term() ||
      (request.last_log_term() == log_.back().term() &&
       request.last_log_index() >= log_.size() - 1)) {
    response.set_vote_granted(true);
    leader_id_ = request.candidate_id();
    storage_->SaveVotedFor(leader_id_);
  } else {
    response.set_vote_granted(false);
  }

  network_->SendVoteResponse(request.candidate_id(), response);
}

void RaftStateMachine::HandleVoteResponse(const VoteResponse &response) {
  if (!is_initialized_ || is_terminated_) {
    return;
  }

  if (response.term() > current_term_) {
    current_term_ = response.term();
    leader_id_ = "";
    storage_->SaveCurrentTerm(current_term_);
    return;
  }

  if (response.vote_granted()) {
    auto follower = followers_.find(response.follower_id());
    if (follower != followers_.end()) {
      follower->second.votes_received++;
      if (follower->second.votes_received >= (followers_.size() + 1) / 2) {
        HandleLeaderElection(true);
      }
    }
  }
}

void RaftStateMachine::HandleAppendEntriesRequest(const AppendEntriesRequest &request) {
  if (!is_initialized_ || is_terminated_) {
    return;
  }

  if (request.term() < current_term_) {
    AppendEntriesResponse response;
    response.set_term(current_term_);
    response.set_success(false);
    network_->SendAppendEntriesResponse(request.leader_id(), response);
    return;
  }

  if (request.term() > current_term_) {
    current_term_ = request.term();
    leader_id_ = request.leader_id();
    storage_->SaveCurrentTerm(current_term_);
  }

  AppendEntriesResponse response;
  response.set_term(current_term_);

  // Check if log entries match
  if (request.prev_log_index() >= log_.size() ||
      (request.prev_log_index() >= 0 &&
       log_[request.prev_log_index()].term() != request.prev_log_term())) {
    response.set_success(false);
    response.set_match_index(log_.size() - 1);
  } else {
    // Append new entries
    for (int i = 0; i < request.entries_size(); i++) {
      const auto &entry = request.entries(i);
      if (entry.index() < log_.size() &&
          log_[entry.index()].term() != entry.term()) {
        // Truncate conflicting entries
        log_.resize(entry.index());
        storage_->DeleteLogEntriesFrom(entry.index());
        break;
      }
      if (entry.index() >= log_.size()) {
        log_.push_back(entry);
        storage_->SaveLogEntry(entry);
      }
    }

    // Update commit index
    if (request.leader_commit() > commit_index_) {
      commit_index_ = std::min(request.leader_commit(),
                              static_cast<int64_t>(log_.size() - 1));
      ApplyLogEntries();
    }

    response.set_success(true);
    response.set_match_index(log_.size() - 1);
  }

  network_->SendAppendEntriesResponse(request.leader_id(), response);
}

void RaftStateMachine::HandleAppendEntriesResponse(const AppendEntriesResponse &response) {
  if (!is_initialized_ || is_terminated_) {
    return;
  }

  if (response.term() > current_term_) {
    current_term_ = response.term();
    leader_id_ = "";
    storage_->SaveCurrentTerm(current_term_);
    return;
  }

  if (response.success()) {
    auto follower = followers_.find(response.follower_id());
    if (follower != followers_.end()) {
      follower->second.match_index = response.match_index();
      follower->second.next_index = response.match_index() + 1;
    }
  }
}

void RaftStateMachine::HandleInstallSnapshotRequest(const InstallSnapshotRequest &request) {
  if (!is_initialized_ || is_terminated_) {
    return;
  }

  if (request.term() < current_term_) {
    InstallSnapshotResponse response;
    response.set_term(current_term_);
    response.set_success(false);
    network_->SendInstallSnapshotResponse(request.leader_id(), response);
    return;
  }

  if (request.term() > current_term_) {
    current_term_ = request.term();
    leader_id_ = request.leader_id();
    storage_->SaveCurrentTerm(current_term_);
  }

  InstallSnapshotResponse response;
  response.set_term(current_term_);

  if (request.done()) {
    // Install snapshot
    InstallSnapshot(request.snapshot());
    response.set_success(true);
  }

  network_->SendInstallSnapshotResponse(request.leader_id(), response);
}

void RaftStateMachine::HandleInstallSnapshotResponse(const InstallSnapshotResponse &response) {
  if (!is_initialized_ || is_terminated_) {
    return;
  }

  if (response.term() > current_term_) {
    current_term_ = response.term();
    leader_id_ = "";
    storage_->SaveCurrentTerm(current_term_);
    return;
  }

  if (response.success()) {
    auto follower = followers_.find(response.follower_id());
    if (follower != followers_.end()) {
      follower->second.match_index = log_.size() - 1;
      follower->second.next_index = log_.size();
    }
  }
}

void RaftStateMachine::StartLeaderElection() {
  current_term_++;
  storage_->SaveCurrentTerm(current_term_);

  // Vote for self
  leader_id_ = config_.node_id;
  storage_->SaveVotedFor(leader_id_);

  // Reset follower state
  for (auto &follower : followers_) {
    follower.second.votes_received = 1;
  }

  // Request votes from followers
  VoteRequest request;
  request.set_term(current_term_);
  request.set_candidate_id(config_.node_id);
  request.set_last_log_index(log_.size() - 1);
  request.set_last_log_term(log_.size() > 0 ? log_.back().term() : 0);

  for (const auto &follower : followers_) {
    network_->SendVoteRequest(follower.first, request);
  }
}

void RaftStateMachine::HandleLeaderElection(bool is_leader) {
  if (is_leader) {
    leader_id_ = config_.node_id;
    storage_->SaveVotedFor(leader_id_);

    // Initialize follower state
    for (auto &follower : followers_) {
      follower.second.next_index = log_.size();
      follower.second.match_index = 0;
    }

    // Start sending heartbeats
    SendHeartbeats();
  } else {
    leader_id_ = "";
    storage_->SaveVotedFor(leader_id_);
  }
}

void RaftStateMachine::SendHeartbeats() {
  if (!IsLeader()) {
    return;
  }

  for (const auto &follower : followers_) {
    AppendEntriesRequest request;
    request.set_term(current_term_);
    request.set_leader_id(leader_id_);
    request.set_prev_log_index(follower.second.next_index - 1);
    request.set_prev_log_term(
        follower.second.next_index > 0 ? log_[follower.second.next_index - 1].term() : 0);
    request.set_leader_commit(commit_index_);

    // Include any missing entries
    for (int64_t i = follower.second.next_index; i < log_.size(); i++) {
      request.add_entries()->CopyFrom(log_[i]);
    }

    network_->SendAppendEntriesRequest(follower.first, request);
  }
}

void RaftStateMachine::ApplyLogEntries() {
  while (last_applied_ < commit_index_) {
    last_applied_++;
    const auto &entry = log_[last_applied_];
    entry.update().ApplyTo(current_state_);
  }
}

void RaftStateMachine::CreateSnapshot() {
  Snapshot snapshot;
  snapshot.set_term(current_term_);
  snapshot.set_index(log_.size() - 1);
  snapshot.mutable_state()->CopyFrom(current_state_);

  storage_->SaveSnapshot(snapshot);

  // Truncate log
  log_.erase(log_.begin(), log_.begin() + log_.size() / 2);
  storage_->DeleteLogEntriesFrom(0);
}

void RaftStateMachine::InstallSnapshot(const Snapshot &snapshot) {
  if (snapshot.term() < current_term_) {
    return;
  }

  if (snapshot.term() > current_term_) {
    current_term_ = snapshot.term();
    leader_id_ = "";
    storage_->SaveCurrentTerm(current_term_);
  }

  // Apply snapshot
  current_state_.CopyFrom(snapshot.state());
  last_applied_ = snapshot.index();
  commit_index_ = snapshot.index();

  // Truncate log
  log_.clear();
  storage_->DeleteLogEntriesFrom(0);
}

}  // namespace gcs
}  // namespace ray
 