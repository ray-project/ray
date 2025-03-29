#include "ray/gcs/gcs_server/raft_coordinator.h"

#include <chrono>
#include <random>

#include "ray/common/status.h"
#include "ray/gcs/gcs_server/gcs_server.h"
#include "ray/rpc/raft/raft_client.h"

namespace ray {
namespace gcs {

Status RaftCoordinator::HandleRequestVote(const rpc::RequestVoteRequest &request,
                                        rpc::RequestVoteReply *reply) {
  // If the request's term is less than current term, reject the vote
  if (request.term() < current_term_) {
    reply->set_term(current_term_);
    reply->set_vote_granted(false);
    return Status::OK();
  }

  // If the request's term is greater than current term, update current term
  if (request.term() > current_term_) {
    current_term_ = request.term();
    state_ = RaftState::FOLLOWER;
    voted_for_ = "";
  }

  // If we haven't voted for anyone in this term and the candidate's log is at least
  // as up-to-date as our log, grant the vote
  if (voted_for_.empty() || voted_for_ == request.candidate_id()) {
    // Compare the last log term and index
    if (request.last_log_term() > GetLastLogTerm() ||
        (request.last_log_term() == GetLastLogTerm() &&
         request.last_log_index() >= GetLastLogIndex())) {
      reply->set_term(current_term_);
      reply->set_vote_granted(true);
      voted_for_ = request.candidate_id();
      return Status::OK();
    }
  }

  reply->set_term(current_term_);
  reply->set_vote_granted(false);
  return Status::OK();
}

Status RaftCoordinator::HandleAppendEntries(const rpc::AppendEntriesRequest &request,
                                          rpc::AppendEntriesReply *reply) {
  // If the request's term is less than current term, reject the request
  if (request.term() < current_term_) {
    reply->set_term(current_term_);
    reply->set_success(false);
    return Status::OK();
  }

  // If the request's term is greater than current term, update current term
  if (request.term() > current_term_) {
    current_term_ = request.term();
    state_ = RaftState::FOLLOWER;
    voted_for_ = "";
  }

  // Reset election timer
  if (election_timer_) {
    election_timer_->expires_from_now(boost::posix_time::milliseconds(
        std::rand() % 150 + 150));  // Random timeout between 150-300ms
  }

  // Check if the log contains an entry at prev_log_index with prev_log_term
  if (!CheckLogEntry(request.prev_log_index(), request.prev_log_term())) {
    reply->set_term(current_term_);
    reply->set_success(false);
    return Status::OK();
  }

  // Append any new entries not already in the log
  for (const auto &entry : request.entries()) {
    if (entry.index() >= log_.size()) {
      // Append new entry
      log_.push_back(entry);
    } else if (log_[entry.index()].term() != entry.term()) {
      // Truncate conflicting entries and append new entry
      TruncateLog(entry.index());
      log_.push_back(entry);
    }
  }

  // Update commit index if leader's commit index is greater
  if (request.leader_commit() > commit_index_) {
    commit_index_ = std::min(request.leader_commit(), GetLastLogIndex());
    UpdateCommitIndex();
  }

  reply->set_term(current_term_);
  reply->set_success(true);
  return Status::OK();
}

Status RaftCoordinator::HandleInstallSnapshot(const rpc::InstallSnapshotRequest &request,
                                            rpc::InstallSnapshotReply *reply) {
  // If the request's term is less than current term, reject the request
  if (request.term() < current_term_) {
    reply->set_term(current_term_);
    return Status::OK();
  }

  // If the request's term is greater than current term, update current term
  if (request.term() > current_term_) {
    current_term_ = request.term();
    state_ = RaftState::FOLLOWER;
    voted_for_ = "";
  }

  // Install the snapshot
  InstallSnapshot(request);

  reply->set_term(current_term_);
  return Status::OK();
}

void RaftCoordinator::InstallSnapshot(const rpc::InstallSnapshotRequest &request) {
  // Truncate the log to discard entries before the snapshot
  TruncateLog(request.last_included_index());

  // Update commit index and last applied index
  commit_index_ = request.last_included_index();
  last_applied_ = request.last_included_index();

  // Parse the snapshot data
  rpc::GcsSnapshot snapshot;
  snapshot.ParseFromString(request.snapshot_data());

  // Apply the snapshot to the GCS server's state
  // Clear existing state
  gcs_server_.ClearState();

  // Restore nodes
  for (const auto &node_info : snapshot.nodes()) {
    gcs_server_.AddNode(node_info);
  }

  // Restore jobs
  for (const auto &job_info : snapshot.jobs()) {
    gcs_server_.AddJob(job_info);
  }

  // Restore actors
  for (const auto &actor_info : snapshot.actors()) {
    gcs_server_.AddActor(actor_info);
  }

  // Restore tasks
  for (const auto &task_info : snapshot.tasks()) {
    gcs_server_.AddTask(task_info);
  }

  // Restore objects
  for (const auto &object_info : snapshot.objects()) {
    gcs_server_.AddObject(object_info);
  }

  // Restore cluster configuration
  if (snapshot.has_cluster_config()) {
    gcs_server_.UpdateClusterConfig(snapshot.cluster_config());
  }

  // Restore resource usage
  if (snapshot.has_resource_usage()) {
    gcs_server_.UpdateResourceUsage(snapshot.resource_usage());
  }

  // Restore placement group info
  for (const auto &placement_group_info : snapshot.placement_groups()) {
    gcs_server_.AddPlacementGroup(placement_group_info);
  }

  // Restore namespace info
  for (const auto &namespace_info : snapshot.namespaces()) {
    gcs_server_.AddNamespace(namespace_info);
  }

  // Restore runtime env info
  for (const auto &runtime_env_info : snapshot.runtime_envs()) {
    gcs_server_.AddRuntimeEnv(runtime_env_info);
  }

  // Restore function info
  for (const auto &function_info : snapshot.functions()) {
    gcs_server_.AddFunction(function_info);
  }

  // Restore KV store
  for (const auto &kv_pair : snapshot.kv_store()) {
    gcs_server_.PutKeyValue(kv_pair.first, kv_pair.second);
  }
}

void RaftCoordinator::Start() {
  // Initialize election timer
  election_timer_ = std::make_unique<boost::asio::deadline_timer>(
      io_context_.get_io_service(),
      boost::posix_time::milliseconds(std::rand() % 150 + 150));  // Random timeout between 150-300ms

  // Start election timer
  election_timer_->async_wait([this](const boost::system::error_code &error) {
    if (!error && state_ == RaftState::FOLLOWER) {
      StartElection();
    }
  });
}

void RaftCoordinator::Stop() {
  if (election_timer_) {
    election_timer_->cancel();
  }
  if (heartbeat_timer_) {
    heartbeat_timer_->cancel();
  }
}

void RaftCoordinator::StartElection() {
  // Increment current term
  current_term_++;
  state_ = RaftState::CANDIDATE;
  voted_for_ = "";

  // Reset election timer
  election_timer_->expires_from_now(boost::posix_time::milliseconds(
      std::rand() % 150 + 150));  // Random timeout between 150-300ms

  // Vote for self
  int votes_received = 1;

  // Send RequestVote RPCs to all other servers
  for (const auto &client_pair : raft_clients_) {
    rpc::RequestVoteRequest request;
    request.set_term(current_term_);
    request.set_candidate_id(gcs_server_.GetClusterId().Binary());

    client_pair.second->RequestVote(
        request,
        [this, votes_received](const Status &status, const rpc::RequestVoteReply &reply) {
          if (status.ok() && reply.vote_granted()) {
            votes_received++;
            // If we received votes from majority of servers, become leader
            if (votes_received > raft_clients_.size() / 2) {
              state_ = RaftState::LEADER;
              SendHeartbeat();
            }
          }
        });
  }
}

void RaftCoordinator::SendHeartbeat() {
  if (state_ != RaftState::LEADER) {
    return;
  }

  // Send AppendEntries RPCs to all followers
  for (const auto &client_pair : raft_clients_) {
    rpc::AppendEntriesRequest request;
    request.set_term(current_term_);
    request.set_leader_id(gcs_server_.GetClusterId().Binary());
    request.set_prev_log_index(commit_index_);
    request.set_prev_log_term(current_term_);
    request.set_leader_commit(commit_index_);

    client_pair.second->AppendEntries(
        request,
        [this, &client_pair](const Status &status, const rpc::AppendEntriesReply &reply) {
          if (status.ok()) {
            if (reply.success()) {
              // Update nextIndex and matchIndex for the follower
              next_index_[client_pair.first] = GetLastLogIndex() + 1;
              match_index_[client_pair.first] = GetLastLogIndex();
            } else {
              // Decrement nextIndex and retry
              next_index_[client_pair.first] = std::max(0L, next_index_[client_pair.first] - 1);
            }
          }
        });
  }

  // Schedule next heartbeat
  heartbeat_timer_ = std::make_unique<boost::asio::deadline_timer>(
      io_context_.get_io_service(), boost::posix_time::milliseconds(50));  // 50ms heartbeat interval
  heartbeat_timer_->async_wait([this](const boost::system::error_code &error) {
    if (!error) {
      SendHeartbeat();
    }
  });
}

void RaftCoordinator::UpdateCommitIndex() {
  if (state_ != RaftState::LEADER) {
    return;
  }

  // Find the highest log entry that is replicated on majority of servers
  std::vector<int64_t> match_indices;
  for (const auto &pair : match_index_) {
    match_indices.push_back(pair.second);
  }
  std::sort(match_indices.begin(), match_indices.end());

  // Update commit index if there is a majority
  if (!match_indices.empty()) {
    int64_t majority_index = match_indices[match_indices.size() / 2];
    if (majority_index > commit_index_ && CheckLogEntry(majority_index, current_term_)) {
      commit_index_ = majority_index;
    }
  }

  // Apply all entries between last_applied_ and commit_index_ to the state machine
  while (last_applied_ < commit_index_) {
    last_applied_++;
    ApplyLogEntry(log_[last_applied_]);
  }
}

void RaftCoordinator::ApplyLogEntry(const rpc::RaftLogEntry &entry) {
  // Parse the command from the log entry
  const auto &command = entry.command();
  
  // Apply the command to the GCS server's state based on the command type
  switch (command.type()) {
    case rpc::CommandType::NODE_ADD: {
      // Add a new node to the cluster
      rpc::NodeInfo node_info;
      node_info.ParseFromString(command.data());
      gcs_server_.AddNode(node_info);
      break;
    }
    case rpc::CommandType::NODE_REMOVE: {
      // Remove a node from the cluster
      rpc::NodeID node_id;
      node_id.ParseFromString(command.data());
      gcs_server_.RemoveNode(node_id);
      break;
    }
    case rpc::CommandType::JOB_ADD: {
      // Add a new job to the cluster
      rpc::JobInfo job_info;
      job_info.ParseFromString(command.data());
      gcs_server_.AddJob(job_info);
      break;
    }
    case rpc::CommandType::JOB_REMOVE: {
      // Remove a job from the cluster
      rpc::JobID job_id;
      job_id.ParseFromString(command.data());
      gcs_server_.RemoveJob(job_id);
      break;
    }
    case rpc::CommandType::ACTOR_ADD: {
      // Add a new actor to the cluster
      rpc::ActorInfo actor_info;
      actor_info.ParseFromString(command.data());
      gcs_server_.AddActor(actor_info);
      break;
    }
    case rpc::CommandType::ACTOR_REMOVE: {
      // Remove an actor from the cluster
      rpc::ActorID actor_id;
      actor_id.ParseFromString(command.data());
      gcs_server_.RemoveActor(actor_id);
      break;
    }
    case rpc::CommandType::TASK_ADD: {
      // Add a new task to the cluster
      rpc::TaskInfo task_info;
      task_info.ParseFromString(command.data());
      gcs_server_.AddTask(task_info);
      break;
    }
    case rpc::CommandType::TASK_REMOVE: {
      // Remove a task from the cluster
      rpc::TaskID task_id;
      task_id.ParseFromString(command.data());
      gcs_server_.RemoveTask(task_id);
      break;
    }
    case rpc::CommandType::OBJECT_ADD: {
      // Add a new object to the cluster
      rpc::ObjectInfo object_info;
      object_info.ParseFromString(command.data());
      gcs_server_.AddObject(object_info);
      break;
    }
    case rpc::CommandType::OBJECT_REMOVE: {
      // Remove an object from the cluster
      rpc::ObjectID object_id;
      object_id.ParseFromString(command.data());
      gcs_server_.RemoveObject(object_id);
      break;
    }
    default:
      RAY_LOG(ERROR) << "Unknown command type: " << command.type();
      break;
  }
}

void RaftCoordinator::TruncateLog(int64_t index) {
  if (index < 0) {
    return;
  }
  log_.resize(index + 1);
}

}  // namespace gcs
}  // namespace ray 