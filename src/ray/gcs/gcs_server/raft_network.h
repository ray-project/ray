#pragma once

#include <boost/asio.hpp>
#include <functional>
#include <memory>
#include <string>
#include <unordered_map>

#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/status.h"
#include "ray/gcs/gcs_server/raft_types.h"
#include "ray/protobuf/raft.pb.h"

namespace ray {
namespace gcs {

/// \class RaftNetwork
/// The network interface for Raft protocol.
class RaftNetwork {
 public:
  using VoteRequestCallback = std::function<void(const VoteRequest &request)>;
  using VoteResponseCallback = std::function<void(const VoteResponse &response)>;
  using AppendEntriesRequestCallback =
      std::function<void(const AppendEntriesRequest &request)>;
  using AppendEntriesResponseCallback =
      std::function<void(const AppendEntriesResponse &response)>;
  using InstallSnapshotRequestCallback =
      std::function<void(const InstallSnapshotRequest &request)>;
  using InstallSnapshotResponseCallback =
      std::function<void(const InstallSnapshotResponse &response)>;

  RaftNetwork(instrumented_io_context &io_context, const RaftConfig &config);
  virtual ~RaftNetwork() = default;

  /// Initialize the network.
  Status Initialize();

  /// Terminate the network.
  Status Terminate();

  /// Send a vote request to the target node.
  void SendVoteRequest(const std::string &target_node_id, const VoteRequest &request);

  /// Send a vote response to the target node.
  void SendVoteResponse(const std::string &target_node_id, const VoteResponse &response);

  /// Send an append entries request to the target node.
  void SendAppendEntriesRequest(const std::string &target_node_id,
                               const AppendEntriesRequest &request);

  /// Send an append entries response to the target node.
  void SendAppendEntriesResponse(const std::string &target_node_id,
                                const AppendEntriesResponse &response);

  /// Send an install snapshot request to the target node.
  void SendInstallSnapshotRequest(const std::string &target_node_id,
                                 const InstallSnapshotRequest &request);

  /// Send an install snapshot response to the target node.
  void SendInstallSnapshotResponse(const std::string &target_node_id,
                                  const InstallSnapshotResponse &response);

  /// Set the callback for handling vote requests.
  void SetVoteRequestCallback(VoteRequestCallback callback);

  /// Set the callback for handling vote responses.
  void SetVoteResponseCallback(VoteResponseCallback callback);

  /// Set the callback for handling append entries requests.
  void SetAppendEntriesRequestCallback(AppendEntriesRequestCallback callback);

  /// Set the callback for handling append entries responses.
  void SetAppendEntriesResponseCallback(AppendEntriesResponseCallback callback);

  /// Set the callback for handling install snapshot requests.
  void SetInstallSnapshotRequestCallback(InstallSnapshotRequestCallback callback);

  /// Set the callback for handling install snapshot responses.
  void SetInstallSnapshotResponseCallback(InstallSnapshotResponseCallback callback);

 private:
  /// The IO context for asynchronous operations.
  instrumented_io_context &io_context_;

  /// The Raft configuration.
  const RaftConfig &config_;

  /// The TCP acceptor for incoming connections.
  boost::asio::ip::tcp::acceptor acceptor_;

  /// The map of node ID to TCP socket.
  std::unordered_map<std::string, std::shared_ptr<boost::asio::ip::tcp::socket>> sockets_;

  /// The callback for handling vote requests.
  VoteRequestCallback vote_request_callback_;

  /// The callback for handling vote responses.
  VoteResponseCallback vote_response_callback_;

  /// The callback for handling append entries requests.
  AppendEntriesRequestCallback append_entries_request_callback_;

  /// The callback for handling append entries responses.
  AppendEntriesResponseCallback append_entries_response_callback_;

  /// The callback for handling install snapshot requests.
  InstallSnapshotRequestCallback install_snapshot_request_callback_;

  /// The callback for handling install snapshot responses.
  InstallSnapshotResponseCallback install_snapshot_response_callback_;

  /// Start accepting incoming connections.
  void StartAccept();

  /// Handle a new connection.
  void HandleAccept(const boost::system::error_code &error,
                   std::shared_ptr<boost::asio::ip::tcp::socket> socket);

  /// Handle a message received from a node.
  void HandleMessage(const std::string &node_id,
                    const boost::system::error_code &error,
                    size_t bytes_transferred);
};

}  // namespace gcs
}  // namespace ray 