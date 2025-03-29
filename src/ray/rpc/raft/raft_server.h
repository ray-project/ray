#pragma once

#include <grpcpp/grpcpp.h>

#include "ray/common/status.h"
#include "ray/rpc/grpc_server.h"
#include "ray/rpc/server_call.h"
#include "src/ray/protobuf/raft.grpc.pb.h"
#include "src/ray/protobuf/raft.pb.h"

namespace ray {
namespace rpc {

#define RAFT_SERVICE_RPC_HANDLER(HANDLER) \
  RPC_SERVICE_HANDLER(RaftService, HANDLER, -1)

/// Interface of the `RaftService`, see `src/ray/protobuf/raft.proto`.
class RaftServiceHandler {
 public:
  virtual ~RaftServiceHandler() = default;

  /// Handle a `RequestVote` request.
  /// The implementation can handle this request asynchronously. When handling is done,
  /// the `send_reply_callback` should be called.
  ///
  /// \param[in] request The request message.
  /// \param[out] reply The reply message.
  /// \param[in] send_reply_callback The callback to be called when the request is done.
  virtual void HandleRequestVote(RequestVoteRequest request,
                                RequestVoteReply *reply,
                                SendReplyCallback send_reply_callback) = 0;

  /// Handle an `AppendEntries` request.
  /// The implementation can handle this request asynchronously. When handling is done,
  /// the `send_reply_callback` should be called.
  ///
  /// \param[in] request The request message.
  /// \param[out] reply The reply message.
  /// \param[in] send_reply_callback The callback to be called when the request is done.
  virtual void HandleAppendEntries(AppendEntriesRequest request,
                                  AppendEntriesReply *reply,
                                  SendReplyCallback send_reply_callback) = 0;

  /// Handle an `InstallSnapshot` request.
  /// The implementation can handle this request asynchronously. When handling is done,
  /// the `send_reply_callback` should be called.
  ///
  /// \param[in] request The request message.
  /// \param[out] reply The reply message.
  /// \param[in] send_reply_callback The callback to be called when the request is done.
  virtual void HandleInstallSnapshot(InstallSnapshotRequest request,
                                    InstallSnapshotReply *reply,
                                    SendReplyCallback send_reply_callback) = 0;
};

/// The `GrpcService` for `RaftService`.
class RaftGrpcService : public GrpcService {
 public:
  /// Constructor.
  ///
  /// \param[in] io_service See super class.
  /// \param[in] handler The service handler that actually handle the requests.
  RaftGrpcService(instrumented_io_context &io_service,
                  RaftServiceHandler &service_handler)
      : GrpcService(io_service), service_handler_(service_handler){};

 protected:
  grpc::Service &GetGrpcService() override { return service_; }

  void InitServerCallFactories(
      const std::unique_ptr<grpc::ServerCompletionQueue> &cq,
      std::vector<std::unique_ptr<ServerCallFactory>> *server_call_factories,
      const ClusterID &cluster_id) override {
    RAFT_SERVICE_RPC_HANDLER(RequestVote);
    RAFT_SERVICE_RPC_HANDLER(AppendEntries);
    RAFT_SERVICE_RPC_HANDLER(InstallSnapshot);
  }

 private:
  /// The grpc async service object.
  RaftService::AsyncService service_;
  /// The service handler that actually handle the requests.
  RaftServiceHandler &service_handler_;
};

}  // namespace rpc
}  // namespace ray 