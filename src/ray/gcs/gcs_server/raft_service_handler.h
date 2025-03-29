#pragma once

#include <memory>

#include "ray/gcs/gcs_server/gcs_server.h"
#include "ray/gcs/gcs_server/raft_coordinator.h"
#include "ray/rpc/raft/raft_server.h"

namespace ray {
namespace gcs {

/// RaftServiceHandler implements the RaftServiceHandler interface.
/// It handles Raft protocol messages and coordinates with the GCS server.
class RaftServiceHandler : public rpc::RaftServiceHandler {
 public:
  /// Constructor.
  ///
  /// \param[in] gcs_server The GCS server instance.
  explicit RaftServiceHandler(GcsServer &gcs_server);

  void HandleRequestVote(rpc::RequestVoteRequest request,
                        rpc::RequestVoteReply *reply,
                        rpc::SendReplyCallback send_reply_callback) override;

  void HandleAppendEntries(rpc::AppendEntriesRequest request,
                          rpc::AppendEntriesReply *reply,
                          rpc::SendReplyCallback send_reply_callback) override;

  void HandleInstallSnapshot(rpc::InstallSnapshotRequest request,
                            rpc::InstallSnapshotReply *reply,
                            rpc::SendReplyCallback send_reply_callback) override;

 private:
  /// Reference to the GCS server.
  GcsServer &gcs_server_;

  /// The Raft coordinator that manages the Raft protocol state.
  std::unique_ptr<RaftCoordinator> raft_coordinator_;
};

}  // namespace gcs
}  // namespace ray 