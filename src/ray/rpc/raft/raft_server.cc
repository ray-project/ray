#include "ray/rpc/raft/raft_server.h"

#include <grpcpp/grpcpp.h>

#include "ray/common/status.h"
#include "ray/rpc/grpc_server.h"
#include "ray/rpc/server_call.h"
#include "src/ray/protobuf/raft.grpc.pb.h"
#include "src/ray/protobuf/raft.pb.h"

namespace ray {
namespace rpc {

void RaftServiceHandler::HandleRequestVote(RequestVoteRequest request,
                                         RequestVoteReply *reply,
                                         SendReplyCallback send_reply_callback) {
  // TODO: Implement RequestVote handler
  send_reply_callback(Status::OK(), nullptr, nullptr);
}

void RaftServiceHandler::HandleAppendEntries(AppendEntriesRequest request,
                                           AppendEntriesReply *reply,
                                           SendReplyCallback send_reply_callback) {
  // TODO: Implement AppendEntries handler
  send_reply_callback(Status::OK(), nullptr, nullptr);
}

void RaftServiceHandler::HandleInstallSnapshot(InstallSnapshotRequest request,
                                             InstallSnapshotReply *reply,
                                             SendReplyCallback send_reply_callback) {
  // TODO: Implement InstallSnapshot handler
  send_reply_callback(Status::OK(), nullptr, nullptr);
}

}  // namespace rpc
}  // namespace ray 