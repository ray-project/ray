#pragma once

#include <memory>
#include <string>
#include <vector>

#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/id.h"
#include "ray/gcs/gcs_server/gcs_server.h"
#include "ray/gcs/redis_client.h"
#include "ray/util/logging.h"
#include "ray/common/status.h"
#include "ray/gcs/gcs_server/raft_types.h"
#include "ray/gcs/gcs_server/raft_impl.h"

namespace ray {
namespace gcs {

/// \class RaftGcsCoordinator
/// Coordinates GCS state using Raft consensus
class RaftGcsCoordinator {
 public:
  /// \brief Construct a new Raft GCS coordinator
  /// \param config The Raft configuration
  explicit RaftGcsCoordinator(const RaftConfig &config);

  /// \brief Start the coordinator
  void Start();

  /// \brief Stop the coordinator
  void Stop();

  /// \brief Submit a state update
  /// \param update The state update to submit
  /// \return Status indicating success or failure
  Status SubmitUpdate(const StateUpdate &update);

  /// \brief Sync state with followers
  /// \param state The state to sync
  void SyncState(const ConsensusState &state);

  /// \brief Get the current term
  /// \return The current term
  int64_t GetCurrentTerm() const;

  /// \brief Get the leader ID
  /// \return The leader ID
  std::string GetLeaderId() const;

  /// \brief Check if this node is the leader
  /// \return True if this node is the leader
  bool IsLeader() const;

 private:
  /// \brief Handle leader election result
  /// \param is_leader Whether this node became leader
  void HandleLeaderElection(bool is_leader);

  /// The Raft configuration
  RaftConfig config_;

  /// Whether the coordinator is started
  bool is_started_;

  /// Whether the coordinator is stopped
  bool is_stopped_;

  /// The Raft group
  std::unique_ptr<RaftGroup<ConsensusState>> raft_group_;
};

}  // namespace gcs
}  // namespace ray 