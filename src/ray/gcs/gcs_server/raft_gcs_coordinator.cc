#include "ray/gcs/gcs_server/raft_gcs_coordinator.h"

#include <memory>
#include <string>

#include "ray/common/asio/asio_util.h"
#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/status.h"
#include "ray/gcs/gcs_server/raft_types.h"
#include "ray/util/logging.h"

namespace ray {
namespace gcs {

RaftGcsCoordinator::RaftGcsCoordinator(const RaftConfig &config)
    : config_(config),
      is_started_(false),
      is_stopped_(false) {
  // Initialize Raft group
  raft_group_ = std::make_unique<RaftGroup<ConsensusState>>(
      config.election_timeout,
      config.heartbeat_interval,
      [this](bool is_leader) {
        HandleLeaderElection(is_leader);
      });
}

void RaftGcsCoordinator::Start() {
  if (is_started_) {
    return;
  }

  // Start Raft group
  raft_group_->Start();

  is_started_ = true;
}

void RaftGcsCoordinator::Stop() {
  if (is_stopped_) {
    return;
  }

  // Stop Raft group
  raft_group_->Stop();

  is_stopped_ = true;
}

Status RaftGcsCoordinator::SubmitUpdate(const StateUpdate &update) {
  return raft_group_->SubmitUpdate(update);
}

void RaftGcsCoordinator::SyncState(const ConsensusState &state) {
  raft_group_->SyncState(state);
}

int64_t RaftGcsCoordinator::GetCurrentTerm() const {
  return raft_group_->GetCurrentTerm();
}

std::string RaftGcsCoordinator::GetLeaderId() const {
  return raft_group_->GetLeaderId();
}

bool RaftGcsCoordinator::IsLeader() const {
  return raft_group_->IsLeader();
}

void RaftGcsCoordinator::HandleLeaderElection(bool is_leader) {
  if (is_leader) {
    RAY_LOG(INFO) << "Node " << config_.node_id << " became leader";
  } else {
    RAY_LOG(INFO) << "Node " << config_.node_id << " became follower";
  }
}

}  // namespace gcs
}  // namespace ray 