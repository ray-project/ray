#include "ray/gcs/gcs_server/raft_network.h"

#include <boost/asio.hpp>
#include <memory>
#include <string>

#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/status.h"
#include "ray/gcs/gcs_server/raft_types.h"
#include "ray/protobuf/raft.pb.h"
#include "ray/util/logging.h"

namespace ray {
namespace gcs {

RaftNetwork::RaftNetwork(instrumented_io_context &io_context, const RaftConfig &config)
    : io_context_(io_context),
      config_(config),
      acceptor_(io_context) {}

Status RaftNetwork::Initialize() {
  try {
    // Parse the node address to get the port
    auto pos = config_.node_addresses[0].find(':');
    if (pos == std::string::npos) {
      return Status::Invalid("Invalid node address format");
    }
    auto port = std::stoi(config_.node_addresses[0].substr(pos + 1));

    // Start accepting connections
    boost::asio::ip::tcp::endpoint endpoint(boost::asio::ip::tcp::v4(), port);
    acceptor_.open(endpoint.protocol());
    acceptor_.set_option(boost::asio::ip::tcp::acceptor::reuse_address(true));
    acceptor_.bind(endpoint);
    acceptor_.listen();

    StartAccept();

    return Status::OK();
  } catch (const std::exception &e) {
    return Status::IOError(e.what());
  }
}

Status RaftNetwork::Terminate() {
  try {
    acceptor_.close();
    for (auto &socket : sockets_) {
      socket.second->close();
    }
    sockets_.clear();
    return Status::OK();
  } catch (const std::exception &e) {
    return Status::IOError(e.what());
  }
}

void RaftNetwork::SendVoteRequest(const std::string &target_node_id,
                                const VoteRequest &request) {
  auto socket = sockets_[target_node_id];
  if (!socket) {
    // Connect to the target node
    auto pos = config_.node_addresses[0].find(':');
    if (pos == std::string::npos) {
      RAY_LOG(ERROR) << "Invalid node address format";
      return;
    }
    auto host = config_.node_addresses[0].substr(0, pos);
    auto port = std::stoi(config_.node_addresses[0].substr(pos + 1));

    socket = std::make_shared<boost::asio::ip::tcp::socket>(io_context_);
    boost::asio::ip::tcp::endpoint endpoint(
        boost::asio::ip::address::from_string(host), port);
    socket->connect(endpoint);
    sockets_[target_node_id] = socket;
  }

  // Serialize the request
  std::string data;
  request.SerializeToString(&data);

  // Send the request
  boost::asio::write(*socket, boost::asio::buffer(data));
}

void RaftNetwork::SendVoteResponse(const std::string &target_node_id,
                                 const VoteResponse &response) {
  auto socket = sockets_[target_node_id];
  if (!socket) {
    RAY_LOG(ERROR) << "Socket not found for node " << target_node_id;
    return;
  }

  // Serialize the response
  std::string data;
  response.SerializeToString(&data);

  // Send the response
  boost::asio::write(*socket, boost::asio::buffer(data));
}

void RaftNetwork::SendAppendEntriesRequest(const std::string &target_node_id,
                                         const AppendEntriesRequest &request) {
  auto socket = sockets_[target_node_id];
  if (!socket) {
    RAY_LOG(ERROR) << "Socket not found for node " << target_node_id;
    return;
  }

  // Serialize the request
  std::string data;
  request.SerializeToString(&data);

  // Send the request
  boost::asio::write(*socket, boost::asio::buffer(data));
}

void RaftNetwork::SendAppendEntriesResponse(const std::string &target_node_id,
                                          const AppendEntriesResponse &response) {
  auto socket = sockets_[target_node_id];
  if (!socket) {
    RAY_LOG(ERROR) << "Socket not found for node " << target_node_id;
    return;
  }

  // Serialize the response
  std::string data;
  response.SerializeToString(&data);

  // Send the response
  boost::asio::write(*socket, boost::asio::buffer(data));
}

void RaftNetwork::SendInstallSnapshotRequest(const std::string &target_node_id,
                                           const InstallSnapshotRequest &request) {
  auto socket = sockets_[target_node_id];
  if (!socket) {
    RAY_LOG(ERROR) << "Socket not found for node " << target_node_id;
    return;
  }

  // Serialize the request
  std::string data;
  request.SerializeToString(&data);

  // Send the request
  boost::asio::write(*socket, boost::asio::buffer(data));
}

void RaftNetwork::SendInstallSnapshotResponse(const std::string &target_node_id,
                                            const InstallSnapshotResponse &response) {
  auto socket = sockets_[target_node_id];
  if (!socket) {
    RAY_LOG(ERROR) << "Socket not found for node " << target_node_id;
    return;
  }

  // Serialize the response
  std::string data;
  response.SerializeToString(&data);

  // Send the response
  boost::asio::write(*socket, boost::asio::buffer(data));
}

void RaftNetwork::SetVoteRequestCallback(VoteRequestCallback callback) {
  vote_request_callback_ = std::move(callback);
}

void RaftNetwork::SetVoteResponseCallback(VoteResponseCallback callback) {
  vote_response_callback_ = std::move(callback);
}

void RaftNetwork::SetAppendEntriesRequestCallback(AppendEntriesRequestCallback callback) {
  append_entries_request_callback_ = std::move(callback);
}

void RaftNetwork::SetAppendEntriesResponseCallback(AppendEntriesResponseCallback callback) {
  append_entries_response_callback_ = std::move(callback);
}

void RaftNetwork::SetInstallSnapshotRequestCallback(
    InstallSnapshotRequestCallback callback) {
  install_snapshot_request_callback_ = std::move(callback);
}

void RaftNetwork::SetInstallSnapshotResponseCallback(
    InstallSnapshotResponseCallback callback) {
  install_snapshot_response_callback_ = std::move(callback);
}

void RaftNetwork::StartAccept() {
  auto socket = std::make_shared<boost::asio::ip::tcp::socket>(io_context_);
  acceptor_.async_accept(*socket,
                        [this, socket](const boost::system::error_code &error) {
                          HandleAccept(error, socket);
                        });
}

void RaftNetwork::HandleAccept(const boost::system::error_code &error,
                             std::shared_ptr<boost::asio::ip::tcp::socket> socket) {
  if (!error) {
    // Start reading from the socket
    auto buffer = std::make_shared<std::vector<char>>(1024);
    socket->async_read_some(
        boost::asio::buffer(*buffer),
        [this, socket, buffer](const boost::system::error_code &error,
                             size_t bytes_transferred) {
          HandleMessage(socket->remote_endpoint().address().to_string(), error,
                       bytes_transferred);
        });

    // Accept the next connection
    StartAccept();
  } else {
    RAY_LOG(ERROR) << "Error accepting connection: " << error.message();
  }
}

void RaftNetwork::HandleMessage(const std::string &node_id,
                              const boost::system::error_code &error,
                              size_t bytes_transferred) {
  if (!error) {
    // TODO: Parse the message and call the appropriate callback
  } else {
    RAY_LOG(ERROR) << "Error reading from socket: " << error.message();
  }
}

}  // namespace gcs
}  // namespace ray 