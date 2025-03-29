#pragma once

#include <grpcpp/grpcpp.h>

#include "ray/common/status.h"
#include "ray/rpc/client_call.h"
#include "src/ray/protobuf/raft.grpc.pb.h"
#include "src/ray/protobuf/raft.pb.h"

namespace ray {
namespace rpc {

/// Client used for communicating with Raft service.
class RaftClient {
 public:
  /// Constructor.
  ///
  /// \param[in] address Address of the Raft service.
  /// \param[in] port Port of the Raft service.
  /// \param[in] client_call_manager The `ClientCallManager` used for managing requests.
  RaftClient(const std::string &address,
             const int port,
             ClientCallManager &client_call_manager)
      : address_(address),
        port_(port),
        client_call_manager_(client_call_manager) {
    grpc::ChannelArguments arguments;
    arguments.SetInt(GRPC_ARG_MAX_RECONNECT_BACKOFF_MS,
                     ::RayConfig::instance().gcs_grpc_max_reconnect_backoff_ms());
    arguments.SetInt(GRPC_ARG_MIN_RECONNECT_BACKOFF_MS,
                     ::RayConfig::instance().gcs_grpc_min_reconnect_backoff_ms());
    arguments.SetInt(GRPC_ARG_INITIAL_RECONNECT_BACKOFF_MS,
                     ::RayConfig::instance().gcs_grpc_initial_reconnect_backoff_ms());
    std::shared_ptr<grpc::Channel> channel =
        grpc::CreateCustomChannel(address + ":" + std::to_string(port),
                                grpc::InsecureChannelCredentials(),
                                arguments);
    stub_ = RaftService::NewStub(channel);
  }

  /// RequestVote RPC
  void RequestVote(const RequestVoteRequest &request,
                   const ClientCallback<RequestVoteReply> &callback,
                   const int64_t timeout_ms = -1) {
    invoke_async_method<RaftService,
                        RequestVoteRequest,
                        RequestVoteReply,
                        false>(&RaftService::Stub::PrepareAsyncRequestVote,
                              stub_,
                              "RaftService.grpc_client.RequestVote",
                              request,
                              callback,
                              timeout_ms);
  }

  /// AppendEntries RPC
  void AppendEntries(const AppendEntriesRequest &request,
                     const ClientCallback<AppendEntriesReply> &callback,
                     const int64_t timeout_ms = -1) {
    invoke_async_method<RaftService,
                        AppendEntriesRequest,
                        AppendEntriesReply,
                        false>(&RaftService::Stub::PrepareAsyncAppendEntries,
                              stub_,
                              "RaftService.grpc_client.AppendEntries",
                              request,
                              callback,
                              timeout_ms);
  }

  /// InstallSnapshot RPC
  void InstallSnapshot(const InstallSnapshotRequest &request,
                       const ClientCallback<InstallSnapshotReply> &callback,
                       const int64_t timeout_ms = -1) {
    invoke_async_method<RaftService,
                        InstallSnapshotRequest,
                        InstallSnapshotReply,
                        false>(&RaftService::Stub::PrepareAsyncInstallSnapshot,
                              stub_,
                              "RaftService.grpc_client.InstallSnapshot",
                              request,
                              callback,
                              timeout_ms);
  }

 private:
  /// The address of the Raft service.
  const std::string address_;
  /// The port of the Raft service.
  const int port_;
  /// The `ClientCallManager` used for managing requests.
  ClientCallManager &client_call_manager_;
  /// The gRPC stub.
  std::unique_ptr<RaftService::Stub> stub_;
};

}  // namespace rpc
}  // namespace ray 