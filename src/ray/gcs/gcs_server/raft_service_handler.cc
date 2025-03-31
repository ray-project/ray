#include "ray/gcs/gcs_server/raft_service_handler.h"

#include "ray/common/status.h"
#include "ray/gcs/gcs_server/gcs_server.h"
#include "ray/gcs/gcs_server/raft_coordinator.h"

namespace ray {
namespace gcs {

RaftServiceHandler::RaftServiceHandler(GcsServer &gcs_server)
    : gcs_server_(gcs_server),
      raft_coordinator_(std::make_unique<RaftCoordinator>(
          gcs_server,
          gcs_server.GetIOContext())) {
  raft_coordinator_->Start();
}

void RaftServiceHandler::HandleRequestVote(rpc::RequestVoteRequest request,
                                         rpc::RequestVoteReply *reply,
                                         rpc::SendReplyCallback send_reply_callback) {
  Status status = raft_coordinator_->HandleRequestVote(request, reply);
  send_reply_callback(status, nullptr, nullptr);
}

void RaftServiceHandler::HandleAppendEntries(rpc::AppendEntriesRequest request,
                                           rpc::AppendEntriesReply *reply,
                                           rpc::SendReplyCallback send_reply_callback) {
  Status status = raft_coordinator_->HandleAppendEntries(request, reply);
  send_reply_callback(status, nullptr, nullptr);
}

void RaftServiceHandler::HandleInstallSnapshot(rpc::InstallSnapshotRequest request,
                                             rpc::InstallSnapshotReply *reply,
                                             rpc::SendReplyCallback send_reply_callback) {
  Status status = raft_coordinator_->HandleInstallSnapshot(request, reply);
  send_reply_callback(status, nullptr, nullptr);
}

}  // namespace gcs
}  // namespace ray 