#include "ray/gcs/gcs_server/raft_impl.h"

#include <memory>
#include <string>
#include <vector>

#include "ray/common/asio/asio_util.h"
#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/status.h"
#include "ray/gcs/gcs_server/raft_types.h"
#include "ray/protobuf/raft.pb.h"
#include "ray/util/logging.h"

namespace ray {
namespace gcs {

template <typename State>
RaftGroup<State>::RaftGroup(int64_t election_timeout,
                           int64_t heartbeat_interval,
                           std::function<void(bool)> leader_callback)
    : election_timeout_(election_timeout),
      heartbeat_interval_(heartbeat_interval),
      leader_callback_(leader_callback),
      current_term_(0),
      commit_index_(-1),
      last_applied_(-1),
      is_started_(false),
      is_stopped_(false) {}

template <typename State>
void RaftGroup<State>::Start() {
  if (is_started_) {
    return;
  }

  // Start leader election
  StartLeaderElection();

  // Start heartbeat timer
  io_service_.post(
      [this]() {
        SendHeartbeats();
      },
      heartbeat_interval_);

  is_started_ = true;
}

template <typename State>
void RaftGroup<State>::Stop() {
  if (is_stopped_) {
    return;
  }

  is_stopped_ = true;
}

template <typename State>
Status RaftGroup<State>::SubmitUpdate(const StateUpdate &update) {
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

  // Replicate to followers
  for (const auto &follower : followers_) {
    AppendEntriesRequest request;
    request.set_term(current_term_);
    request.set_leader_id(leader_id_);
    request.set_prev_log_index(log_.size() - 2);
    request.set_prev_log_term(log_.size() > 1 ? log_[log_.size() - 2].term() : 0);
    request.set_leader_commit(commit_index_);
    request.add_entries()->CopyFrom(entry);

    // Send request to follower
    SendAppendEntriesRequest(follower, request);
  }

  return Status::OK();
}

template <typename State>
void RaftGroup<State>::SyncState(const State &state) {
  if (!IsLeader()) {
    return;
  }

  // Create snapshot if needed
  if (log_.size() >= config_.snapshot_interval) {
    CreateSnapshot();
  }

  // Send heartbeats to followers
  SendHeartbeats();
}

template <typename State>
int64_t RaftGroup<State>::GetCurrentTerm() const {
  return current_term_;
}

template <typename State>
std::string RaftGroup<State>::GetLeaderId() const {
  return leader_id_;
}

template <typename State>
bool RaftGroup<State>::IsLeader() const {
  return leader_id_ == config_.node_id;
}

template <typename State>
void RaftGroup<State>::StartLeaderElection() {
  current_term_++;
  votes_received_ = 0;
  votes_needed_ = (followers_.size() + 1) / 2 + 1;

  // Request votes from followers
  RequestVotes();
}

template <typename State>
void RaftGroup<State>::HandleLeaderElection(bool is_leader) {
  if (is_leader) {
    leader_id_ = config_.node_id;
    leader_callback_(true);
  } else {
    leader_id_ = "";
    leader_callback_(false);
  }
}

template <typename State>
void RaftGroup<State>::SendHeartbeats() {
  if (!IsLeader()) {
    return;
  }

  for (const auto &follower : followers_) {
    AppendEntriesRequest request;
    request.set_term(current_term_);
    request.set_leader_id(leader_id_);
    request.set_prev_log_index(log_.size() - 1);
    request.set_prev_log_term(log_.size() > 0 ? log_.back().term() : 0);
    request.set_leader_commit(commit_index_);

    // Send request to follower
    SendAppendEntriesRequest(follower, request);
  }

  // Schedule next heartbeat
  io_service_.post(
      [this]() {
        SendHeartbeats();
      },
      heartbeat_interval_);
}

template <typename State>
void RaftGroup<State>::HandleVoteRequest(const VoteRequest &request) {
  if (request.term() < current_term_) {
    return;
  }

  if (request.term() > current_term_) {
    current_term_ = request.term();
    leader_id_ = "";
  }

  VoteResponse response;
  response.set_term(current_term_);

  // Grant vote if candidate's log is at least as up-to-date as ours
  if (request.last_log_term() > log_.back().term() ||
      (request.last_log_term() == log_.back().term() &&
       request.last_log_index() >= log_.size() - 1)) {
    response.set_vote_granted(true);
    leader_id_ = request.candidate_id();
  } else {
    response.set_vote_granted(false);
  }

  // Send response to candidate
  SendVoteResponse(request.candidate_id(), response);
}

template <typename State>
void RaftGroup<State>::HandleVoteResponse(const VoteResponse &response) {
  if (response.term() > current_term_) {
    current_term_ = response.term();
    leader_id_ = "";
    return;
  }

  if (response.vote_granted()) {
    votes_received_++;
    if (votes_received_ >= votes_needed_) {
      HandleLeaderElection(true);
    }
  }
}

template <typename State>
void RaftGroup<State>::HandleAppendEntriesRequest(const AppendEntriesRequest &request) {
  if (request.term() < current_term_) {
    return;
  }

  if (request.term() > current_term_) {
    current_term_ = request.term();
    leader_id_ = request.leader_id();
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
        break;
      }
      if (entry.index() >= log_.size()) {
        log_.push_back(entry);
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

  // Send response to leader
  SendAppendEntriesResponse(request.leader_id(), response);
}

template <typename State>
void RaftGroup<State>::HandleAppendEntriesResponse(const AppendEntriesResponse &response) {
  if (response.term() > current_term_) {
    current_term_ = response.term();
    leader_id_ = "";
    return;
  }

  if (response.success()) {
    // Update follower's match index
    auto follower = followers_.find(response.follower_id());
    if (follower != followers_.end()) {
      follower->second.match_index = response.match_index();
    }
  }
}

template <typename State>
void RaftGroup<State>::HandleInstallSnapshotRequest(const InstallSnapshotRequest &request) {
  if (request.term() < current_term_) {
    return;
  }

  if (request.term() > current_term_) {
    current_term_ = request.term();
    leader_id_ = request.leader_id();
  }

  InstallSnapshotResponse response;
  response.set_term(current_term_);

  if (request.done()) {
    // Install snapshot
    InstallSnapshot(request.snapshot());
    response.set_success(true);
  }

  // Send response to leader
  SendInstallSnapshotResponse(request.leader_id(), response);
}

template <typename State>
void RaftGroup<State>::HandleInstallSnapshotResponse(const InstallSnapshotResponse &response) {
  if (response.term() > current_term_) {
    current_term_ = response.term();
    leader_id_ = "";
    return;
  }

  if (response.success()) {
    // Update follower's match index
    auto follower = followers_.find(response.follower_id());
    if (follower != followers_.end()) {
      follower->second.match_index = log_.size() - 1;
    }
  }
}

template <typename State>
void RaftGroup<State>::ApplyLogEntries() {
  while (last_applied_ < commit_index_) {
    last_applied_++;
    const auto &entry = log_[last_applied_];
    entry.update().ApplyTo(current_state_);
  }
}

template <typename State>
void RaftGroup<State>::CreateSnapshot() {
  Snapshot snapshot;
  snapshot.set_term(current_term_);
  snapshot.set_index(log_.size() - 1);
  snapshot.mutable_state()->CopyFrom(current_state_);

  // Store snapshot
  StoreSnapshot(snapshot);

  // Truncate log
  log_.erase(log_.begin(), log_.begin() + log_.size() / 2);
}

template <typename State>
void RaftGroup<State>::InstallSnapshot(const Snapshot &snapshot) {
  if (snapshot.term() < current_term_) {
    return;
  }

  if (snapshot.term() > current_term_) {
    current_term_ = snapshot.term();
    leader_id_ = "";
  }

  // Apply snapshot
  current_state_.CopyFrom(snapshot.state());
  last_applied_ = snapshot.index();
  commit_index_ = snapshot.index();

  // Truncate log
  log_.clear();
}

LeaderElection::LeaderElection(int64_t election_timeout,
                              std::function<void(bool)> leader_callback)
    : election_timeout_(election_timeout),
      leader_callback_(leader_callback),
      current_term_(0),
      votes_received_(0),
      votes_needed_(0),
      is_started_(false),
      is_stopped_(false) {}

void LeaderElection::Start() {
  if (is_started_) {
    return;
  }

  // Start election timer
  StartElectionTimer();

  is_started_ = true;
}

void LeaderElection::Stop() {
  if (is_stopped_) {
    return;
  }

  is_stopped_ = true;
}

int64_t LeaderElection::GetCurrentTerm() const {
  return current_term_;
}

std::string LeaderElection::GetLeaderId() const {
  return leader_id_;
}

bool LeaderElection::IsLeader() const {
  return leader_id_ == config_.node_id;
}

void LeaderElection::StartElectionTimer() {
  io_service_.post(
      [this]() {
        HandleElectionTimeout();
      },
      election_timeout_);
}

void LeaderElection::HandleElectionTimeout() {
  if (is_stopped_) {
    return;
  }

  // Start new election
  current_term_++;
  votes_received_ = 0;
  votes_needed_ = (followers_.size() + 1) / 2 + 1;

  // Request votes
  RequestVotes();

  // Restart election timer
  StartElectionTimer();
}

void LeaderElection::RequestVotes() {
  VoteRequest request;
  request.set_term(current_term_);
  request.set_candidate_id(config_.node_id);
  request.set_last_log_index(log_.size() - 1);
  request.set_last_log_term(log_.size() > 0 ? log_.back().term() : 0);

  // Send request to all followers
  for (const auto &follower : followers_) {
    SendVoteRequest(follower.first, request);
  }
}

void LeaderElection::HandleVoteRequest(const VoteRequest &request) {
  if (request.term() < current_term_) {
    return;
  }

  if (request.term() > current_term_) {
    current_term_ = request.term();
    leader_id_ = "";
  }

  VoteResponse response;
  response.set_term(current_term_);

  // Grant vote if candidate's log is at least as up-to-date as ours
  if (request.last_log_term() > log_.back().term() ||
      (request.last_log_term() == log_.back().term() &&
       request.last_log_index() >= log_.size() - 1)) {
    response.set_vote_granted(true);
    leader_id_ = request.candidate_id();
  } else {
    response.set_vote_granted(false);
  }

  // Send response to candidate
  SendVoteResponse(request.candidate_id(), response);
}

void LeaderElection::HandleVoteResponse(const VoteResponse &response) {
  if (response.term() > current_term_) {
    current_term_ = response.term();
    leader_id_ = "";
    return;
  }

  if (response.vote_granted()) {
    votes_received_++;
    if (votes_received_ >= votes_needed_) {
      leader_callback_(true);
    }
  }
}

}  // namespace gcs
}  // namespace ray
 